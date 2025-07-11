import asyncio
import logging
from collections import deque
from enum import Enum, auto
import time
import pandas as pd
import numpy as np

from delta_rollover_bot.config_loader import Config
from delta_rollover_bot.data.data_provider_interface import DataEvent

logger = logging.getLogger(__name__)

# Define pattern stages
class PatternStage(Enum):
    NEUTRAL = auto()
    AGGRESSIVE_IMBALANCE_BULLISH = auto()
    AGGRESSIVE_IMBALANCE_BEARISH = auto()
    ACCUMULATION_BULLISH_SETUP = auto() # After bullish imbalance, expecting pullback to buy
    ACCUMULATION_BEARISH_SETUP = auto() # After bearish imbalance, expecting pullback to sell
    # ROLLOVER_CONFIRMED_LONG_ENTRY = auto() # No, entry is on pullback to broken edge
    # ROLLOVER_CONFIRMED_SHORT_ENTRY = auto()


class PatternDetector:
    """
    Detects the "Delta Rollover -> Pullback" pattern based on Bookmap and SpotGamma data.
    """
    def __init__(self, config: Config, event_publisher: Optional[Callable[[DataEvent], asyncio.Future]] = None):
        self.config = config
        self.instrument = self.config.get('trading.instrument', 'MNQ') # Default to MNQ
        self.tick_size = self.config.get(f'instruments.{self.instrument}.tick_size', 0.25)
        self.event_publisher = event_publisher # Callback to publish pattern events

        # Pattern parameters from config
        # Aggressive Imbalance
        self.delta_threshold_factor = self.config.get('pattern.delta_threshold_factor', 2.0)
        self.avg_delta_minutes = self.config.get('pattern.avg_delta_minutes', 5)
        self.imbalance_ratio_threshold = self.config.get('pattern.imbalance_ratio', 0.65)

        # Accumulation Stage
        self.range_pct_atr_threshold = self.config.get('pattern.range_pct_atr', 0.3)
        self.accum_bars_min = self.config.get('pattern.accum_bars', 5)
        self.flat_delta_threshold_factor = self.config.get('pattern.flat_delta_threshold', 0.1) # Factor of avg delta

        # Rollover Confirmation & Breakout
        self.volume_spike_factor = self.config.get('pattern.volume_spike_factor', 2.0)

        # Internal state
        self.current_stage = PatternStage.NEUTRAL
        self.cumulative_delta = 0.0

        # For calculating average delta over N minutes: store (timestamp, delta_value)
        # We need per-tick delta. Assuming Bookmap provides bid_delta and ask_delta per tick.
        # Total delta for a tick = ask_delta - bid_delta (market buys - market sells)
        self.delta_history_ticks = deque() # Stores (timestamp_ms, tick_total_delta, tick_volume)
        self.max_delta_history_duration_ms = self.avg_delta_minutes * 60 * 1000

        # Tick counters for imbalance ratio
        self.market_buy_ticks = 0
        self.market_sell_ticks = 0
        self.total_ticks_in_period = 0 # For imbalance ratio calculation period (e.g., related to avg_delta_minutes)

        # Data for indicators (ATR, VWAP, RSI) - will be processed typically on bar data
        # For now, let's assume we'll aggregate ticks into 1-minute bars for these
        # This part will need more sophisticated handling (e.g. a BarAggregator class)
        self.bars_df = pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'delta'])
        self.current_bar_data: Optional[Dict] = None
        self.bar_interval_seconds = 60 # e.g. 1-minute bars

        # SpotGamma data (to be updated by external events)
        self.spotgamma_data: Dict[str, Any] = {}

        logger.info(f"PatternDetector initialized for {self.instrument} with params: "
                    f"delta_factor={self.delta_threshold_factor}, avg_delta_min={self.avg_delta_minutes}, "
                    f"imbalance_ratio={self.imbalance_ratio_threshold}")

    def _update_delta_history(self, timestamp_ms: float, tick_total_delta: float, tick_volume: float):
        """Updates the deque storing recent tick deltas and volumes."""
        self.delta_history_ticks.append((timestamp_ms, tick_total_delta, tick_volume))
        # Remove old ticks that are outside the max_delta_history_duration_ms window
        while self.delta_history_ticks and \
              (timestamp_ms - self.delta_history_ticks[0][0] > self.max_delta_history_duration_ms):
            _, removed_delta, removed_volume = self.delta_history_ticks.popleft()
            # Adjust cumulative delta if it was only for this window (it's not, it's session cumulative)
            # Adjust tick counters if they were only for this window
            # For imbalance_ratio, it should be over a dynamic window, not session.
            # This part needs refinement: is imbalance_ratio over the same N minutes? Or different window?
            # Assuming imbalance_ratio is also over `avg_delta_minutes` for now.
            # This requires tracking buy/sell ticks within this deque too.
            # Let's simplify: delta_history_ticks stores (ts, delta, volume, is_buy_dominant)
            # `is_buy_dominant` = 1 if ask_delta > bid_delta, -1 if bid_delta > ask_delta, 0 if equal or no trade

    def _calculate_average_delta_and_volume(self) -> Tuple[float, float]:
        """Calculates average tick delta and average tick volume from delta_history_ticks."""
        if not self.delta_history_ticks:
            return 0.0, 0.0

        sum_delta = sum(d[1] for d in self.delta_history_ticks)
        sum_volume = sum(d[2] for d in self.delta_history_ticks)
        count = len(self.delta_history_ticks)

        avg_delta = sum_delta / count if count > 0 else 0.0
        avg_volume = sum_volume / count if count > 0 else 0.0
        return avg_delta, avg_volume

    def _calculate_imbalance_stats(self) -> Tuple[float, int, int, int]:
        """
        Calculates buy_ticks, sell_ticks, total_ticks in the current window (delta_history_ticks)
        and the imbalance ratio.
        This requires delta_history_ticks to store buy/sell dominance.
        Modify _update_delta_history and its input if necessary. For now, assume tick_total_delta sign.
        """
        if not self.delta_history_ticks:
            return 0.0, 0, 0, 0

        buy_dom_ticks = 0
        sell_dom_ticks = 0

        for _, tick_delta, _, in self.delta_history_ticks: # Assuming tick_delta itself indicates aggressor
            if tick_delta > 0: # Market buys were more aggressive
                buy_dom_ticks += 1
            elif tick_delta < 0: # Market sells were more aggressive
                sell_dom_ticks += 1

        total_window_ticks = len(self.delta_history_ticks)
        current_imbalance_ratio = 0.0
        if total_window_ticks > 0:
            if buy_dom_ticks > sell_dom_ticks:
                current_imbalance_ratio = buy_dom_ticks / total_window_ticks
            elif sell_dom_ticks > buy_dom_ticks:
                current_imbalance_ratio = sell_dom_ticks / total_window_ticks

        return current_imbalance_ratio, buy_dom_ticks, sell_dom_ticks, total_window_ticks

    async def _process_aggressive_imbalance(self, current_price: float, timestamp_ms: float):
        """Checks for Aggressive Imbalance stage conditions."""

        avg_tick_delta, _ = self._calculate_average_delta_and_volume()
        delta_abs_threshold = abs(avg_tick_delta * self.delta_threshold_factor)
        if delta_abs_threshold == 0 and self.cumulative_delta == 0 : # Avoid division by zero or trigger on no activity
             # If avg_tick_delta is zero (e.g. at start), use a small nominal threshold or wait for more data
             # For now, let's require some non-zero avg_tick_delta or non-zero cumulative delta
             # This might need a minimum number of ticks in history too.
            if len(self.delta_history_ticks) < 10: # Arbitrary minimum ticks
                return

        current_imbalance_ratio, buy_ticks, sell_ticks, total_ticks = self._calculate_imbalance_stats()

        # Bullish Imbalance
        if self.cumulative_delta >= delta_abs_threshold and self.cumulative_delta > 0: # Cumulative delta is positive and exceeds threshold
            if buy_ticks > sell_ticks and current_imbalance_ratio >= self.imbalance_ratio_threshold:
                if self.current_stage != PatternStage.AGGRESSIVE_IMBALANCE_BULLISH:
                    logger.info(f"[{self.instrument}@{current_price} ts:{timestamp_ms}] Stage change: NEUTRAL -> AGGRESSIVE_IMBALANCE_BULLISH. "
                                f"CumDelta: {self.cumulative_delta:.2f}, AvgTickDelta: {avg_tick_delta:.2f}, "
                                f"DeltaThreshold: {delta_abs_threshold:.2f}, ImbalanceRatio: {current_imbalance_ratio:.2f} ({buy_ticks}/{total_ticks})")
                    self.current_stage = PatternStage.AGGRESSIVE_IMBALANCE_BULLISH
                    await self._publish_pattern_event("aggressive_imbalance_bullish_detected", {"price": current_price, "cumulative_delta": self.cumulative_delta})
                return # Stay in this stage

        # Bearish Imbalance
        elif self.cumulative_delta <= -delta_abs_threshold and self.cumulative_delta < 0: # Cumulative delta is negative and exceeds threshold (in magnitude)
            if sell_ticks > buy_ticks and current_imbalance_ratio >= self.imbalance_ratio_threshold:
                if self.current_stage != PatternStage.AGGRESSIVE_IMBALANCE_BEARISH:
                    logger.info(f"[{self.instrument}@{current_price} ts:{timestamp_ms}] Stage change: NEUTRAL -> AGGRESSIVE_IMBALANCE_BEARISH. "
                                f"CumDelta: {self.cumulative_delta:.2f}, AvgTickDelta: {avg_tick_delta:.2f}, "
                                f"DeltaThreshold: {delta_abs_threshold:.2f}, ImbalanceRatio: {current_imbalance_ratio:.2f} ({sell_ticks}/{total_ticks})")
                    self.current_stage = PatternStage.AGGRESSIVE_IMBALANCE_BEARISH
                    await self._publish_pattern_event("aggressive_imbalance_bearish_detected", {"price": current_price, "cumulative_delta": self.cumulative_delta})
                return # Stay in this stage

        # If conditions for imbalance are no longer met, and we were in an imbalance stage, transition back to NEUTRAL or to ACCUMULATION
        # For now, let's assume it transitions to ACCUMULATION if imbalance ends.
        # This logic will be refined in the accumulation stage processing.
        # If currently in AGGRESSIVE_IMBALANCE and conditions no longer met, what happens?
        # The pattern implies it moves to ACCUMULATION stage. So, no explicit NEUTRAL here.
        # The transition to ACCUMULATION will be handled by its own logic.

    async def _publish_pattern_event(self, event_name: str, details: Dict):
        if self.event_publisher:
            event_data = {
                "instrument": self.instrument,
                "pattern_stage": self.current_stage.name,
                "details": details
            }
            event = DataEvent(event_type=f"pattern_{event_name}", data=event_data, timestamp=time.time())
            # Ensure publisher is an async function or use asyncio.create_task if it's a regular function
            # For now, assuming event_publisher is awaitable
            await self.event_publisher(event)


    def _aggregate_tick_to_bar(self, timestamp_ms: float, price: float, volume: float, total_delta: float):
        """Aggregates tick data into OHLCV + Delta bars."""
        bar_ts_sec = int(timestamp_ms / 1000 // self.bar_interval_seconds * self.bar_interval_seconds)

        if self.current_bar_data is None or self.current_bar_data["timestamp_sec"] != bar_ts_sec:
            # Finalize previous bar
            if self.current_bar_data is not None:
                new_bar = pd.Series(self.current_bar_data)
                # self.bars_df = self.bars_df.append(new_bar, ignore_index=True) # deprecated
                self.bars_df = pd.concat([self.bars_df, new_bar.to_frame().T], ignore_index=True)

                # TODO: Calculate ATR, VWAP, RSI here or ensure it's done before use
                # self._calculate_indicators()
                # logger.debug(f"New {self.bar_interval_seconds}s bar closed: {self.current_bar_data}")


            # Start new bar
            self.current_bar_data = {
                "timestamp_sec": bar_ts_sec,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 0, # Sum of tick volumes in this bar
                "delta": 0,  # Sum of tick deltas in this bar
                "num_ticks": 0
            }

        # Update current bar
        self.current_bar_data["high"] = max(self.current_bar_data["high"], price)
        self.current_bar_data["low"] = min(self.current_bar_data["low"], price)
        self.current_bar_data["close"] = price
        self.current_bar_data["volume"] += volume
        self.current_bar_data["delta"] += total_delta
        self.current_bar_data["num_ticks"] +=1


    async def on_bookmap_tick(self, event: DataEvent):
        """
        Processes a Bookmap tick data event.
        Expected event.data format:
        {
            "instrument": "MNQ",
            "timestamp_ms": 1678886400123,
            "best_bid": 15000.00,
            "best_ask": 15000.25,
            "traded_volume": 10, # Volume of the last trade(s) aggregated in this tick update
            "bid_delta": 5,   # Total volume traded on bid at this tick update
            "ask_delta": 15,  # Total volume traded on ask at this tick update
            "total_delta": 10 # ask_delta - bid_delta for this tick update
        }
        """
        if event.event_type != "bookmap_tick_data" or event.data.get("instrument") != self.instrument:
            return

        data = event.data
        timestamp_ms = float(data["timestamp_ms"])
        # Use mid-price or last trade price for pattern checks. Let's assume 'best_ask' for bullish context, 'best_bid' for bearish.
        # Or, if there's a last trade price in the tick, use that. Assuming Bookmap gives best_bid/ask.
        # For simplicity, let's use (best_bid + best_ask) / 2 as the current price.
        current_price = (float(data["best_bid"]) + float(data["best_ask"])) / 2.0

        tick_total_delta = float(data["total_delta"]) # This is delta for THIS tick/update
        tick_volume = float(data["traded_volume"])

        # Update cumulative delta for the session/period
        self.cumulative_delta += tick_total_delta

        # Update delta history for avg delta and imbalance ratio calculations
        # The definition of "market buy ticks" vs "market sell ticks" for imbalance ratio needs to be precise.
        # If tick_total_delta > 0, it means more aggressive buying in that tick.
        # If tick_total_delta < 0, it means more aggressive selling in that tick.
        self._update_delta_history(timestamp_ms, tick_total_delta, tick_volume)

        # Aggregate tick to bar
        self._aggregate_tick_to_bar(timestamp_ms, current_price, tick_volume, tick_total_delta)

        # --- Stage Logic ---
        if self.current_stage == PatternStage.NEUTRAL:
            await self._process_aggressive_imbalance(current_price, timestamp_ms)

        elif self.current_stage == PatternStage.AGGRESSIVE_IMBALANCE_BULLISH:
            # Check if still in imbalance, or transition to ACCUMULATION
            # For now, let accumulation stage handle its entry from this.
            # await self._process_accumulation_entry_conditions(current_price, timestamp_ms)
            pass # Placeholder for transition to ACCUMULATION

        elif self.current_stage == PatternStage.AGGRESSIVE_IMBALANCE_BEARISH:
            # await self._process_accumulation_entry_conditions(current_price, timestamp_ms)
            pass # Placeholder for transition to ACCUMULATION

        # Other stages will be handled similarly
        # elif self.current_stage == PatternStage.ACCUMULATION_BULLISH_SETUP:
        #     await self._process_accumulation_bullish(current_price, timestamp_ms)
        # elif self.current_stage == PatternStage.ACCUMULATION_BEARISH_SETUP:
        #     await self._process_accumulation_bearish(current_price, timestamp_ms)

        # Debug log current state frequently if needed
        # logger.debug(f"Tick processed. CumDelta: {self.cumulative_delta:.2f}, Stage: {self.current_stage.name}")


    async def on_spotgamma_update(self, event: DataEvent):
        """
        Processes SpotGamma data updates.
        """
        # Example: event_type="spotgamma_gamma_levels_spx", data={"levels": ..., "index": "SPX"}
        # Store relevant SpotGamma data internally for filter checks
        if event.event_type.startswith("spotgamma_"):
            index = event.data.get("index", "").upper()
            # Assuming pattern detector is for one main instrument (e.g. MNQ which tracks NDX, or ES which tracks SPX)
            # This needs to be configured or inferred.
            # For now, let's assume if self.instrument is MNQ, we care about NDX SpotGamma, if ES then SPX.
            relevant_sg_index = "NDX" if "NQ" in self.instrument.upper() else "SPX"

            if index == relevant_sg_index:
                if "gamma_levels" in event.event_type:
                    self.spotgamma_data['gamma_levels'] = event.data.get("levels")
                    logger.debug(f"Updated SpotGamma gamma_levels for {index}")
                elif "hiro_delta" in event.event_type:
                    self.spotgamma_data['hiro_delta'] = event.data.get("hiro")
                    logger.debug(f"Updated SpotGamma hiro_delta for {index}")
                elif "gamma_flip" in event.event_type:
                    self.spotgamma_data['gamma_flip'] = event.data.get("flip_data") # e.g., {"level": 4500, "type": "call_gamma_flip"}
                    logger.debug(f"Updated SpotGamma gamma_flip for {index}: {self.spotgamma_data['gamma_flip']}")


if __name__ == '__main__': # pragma: no cover
    # Example Usage and Test
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    class DummyConfig:
        def get(self, key, default=None):
            defaults = {
                'trading.instrument': 'MNQ',
                'instruments.MNQ.tick_size': 0.25,
                'pattern.delta_threshold_factor': 1.5, # Lower for easier testing
                'pattern.avg_delta_minutes': 1, # Shorter for testing
                'pattern.imbalance_ratio': 0.60, # Lower for easier testing
            }
            return defaults.get(key, default)

    async def dummy_event_publisher(event: DataEvent):
        logger.info(f"PATTERN_EVENT PUBLISHED: {event.event_type} - {event.data}")

    detector = PatternDetector(config=DummyConfig(), event_publisher=dummy_event_publisher)

    async def test_run():
        # Simulate some Bookmap ticks
        start_time_ms = int(time.time() * 1000)

        # Simulate Bullish Imbalance
        logger.info("--- Simulating Bullish Imbalance ---")
        price = 15000
        for i in range(12): # 12 ticks over 1 minute (5s per tick)
            ts = start_time_ms + i * 5000 # 5 seconds interval
            # Make delta consistently positive and volume moderate
            # bid_d, ask_d controls total_delta and also buy/sell tick counts
            bid_d = 5
            ask_d = 15 + i # Increasing ask delta
            total_d = ask_d - bid_d
            vol = 10 + i

            # Imbalance ratio: need more ask_delta > bid_delta ticks
            # Here, ask_delta is always > bid_delta, so all are "buy_dominant_ticks"
            # Imbalance ratio will be 1.0 if all ticks are buy dominant.

            mock_tick_event = DataEvent(
                event_type="bookmap_tick_data",
                data={
                    "instrument": "MNQ", "timestamp_ms": ts,
                    "best_bid": price, "best_ask": price + 0.25,
                    "traded_volume": vol,
                    "bid_delta": bid_d, "ask_delta": ask_d, # These raw deltas are just for example
                    "total_delta": total_d # This is what the detector uses
                }
            )
            await detector.on_bookmap_tick(mock_tick_event)
            price += 0.25 * (1 if total_d > 0 else -1) # Price moves with delta
            await asyncio.sleep(0.01) # Small delay for processing

        # Reset for next scenario (or create new detector)
        detector.current_stage = PatternStage.NEUTRAL
        detector.cumulative_delta = 0.0
        detector.delta_history_ticks.clear()
        logger.info("--- Simulating Bearish Imbalance ---")
        price = 15100
        for i in range(12):
            ts = start_time_ms + (i + 20) * 5000 # Continue time
            bid_d = 15 + i
            ask_d = 5
            total_d = ask_d - bid_d # Negative
            vol = 10 + i
            mock_tick_event = DataEvent(
                event_type="bookmap_tick_data",
                data={
                    "instrument": "MNQ", "timestamp_ms": ts,
                    "best_bid": price, "best_ask": price + 0.25,
                    "traded_volume": vol,
                    "bid_delta": bid_d, "ask_delta": ask_d,
                    "total_delta": total_d
                }
            )
            await detector.on_bookmap_tick(mock_tick_event)
            price += 0.25 * (1 if total_d > 0 else -1)
            await asyncio.sleep(0.01)

    asyncio.run(test_run())
