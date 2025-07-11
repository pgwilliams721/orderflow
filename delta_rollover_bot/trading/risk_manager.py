import logging
from typing import Dict, Any, Optional, Tuple

from delta_rollover_bot.config_loader import Config
from delta_rollover_bot.trading.broker_interface import Position, OrderSide
# from delta_rollover_bot.trading.trade_gateway import TradeGateway # Would cause circular dependency if gateway imports risk manager for checks

logger = logging.getLogger(__name__)

class RiskManager:
    """
    Manages trading risk, including stop/profit targets, position sizing,
    and prop firm rule compliance.
    """
    def __init__(self, config: Config): # Removed trade_gateway: TradeGateway to avoid circular deps
        self.config = config
        # self.trade_gateway = trade_gateway # Gateway might call RM, RM shouldn't call gateway directly for actions

        # Load risk parameters from config
        self.instrument = self.config.get('trading.instrument', 'MNQ')
        self.tick_size = self.config.get(f'instruments.{self.instrument}.tick_size', 0.25)

        # ATR-based targets/stops
        self.target1_atr_multiplier = self.config.get('trading.target_atr_multiplier', 1.0)
        self.target2_atr_multiplier = self.config.get('trading.target2_atr_multiplier', 2.0)
        self.stop_buffer_ticks = self.config.get('trading.stop_buffer_ticks', 4)
        self.trail_atr_multiplier = self.config.get('trading.trail_atr_multiplier', 1.5)

        # Prop firm rules
        self.max_position_size = self.config.get('trading.max_position_size', 2)
        self.daily_loss_limit_usd = self.config.get('risk_management.daily_loss_limit_usd', 500.0)
        # Trailing drawdown is more complex and needs careful definition (e.g., based on peak equity)
        # self.trailing_drawdown_usd = self.config.get('risk_management.trailing_drawdown_usd', 2500.0)

        # SpotGamma related (for TP2 alternative)
        self.use_spotgamma_tp2 = self.config.get('filters.use_spotgamma_filter', True) # Assuming this implies SG for TP

        self.current_daily_pnl: float = 0.0
        self.active_trade_risks: Dict[str, Dict] = {} # client_order_id -> {stop_price, tp1_price, tp2_price, trailing_active, trail_stop_price}

        logger.info("RiskManager initialized.")
        logger.info(f"Daily Loss Limit: ${self.daily_loss_limit_usd}, Max Position Size: {self.max_position_size} lots")

    def update_account_summary(self, account_summary: Dict[str, Any]):
        """
        Updates risk manager's view of account P&L.
        Called by the main bot logic when account summary updates are received from the gateway.
        """
        self.current_daily_pnl = account_summary.get('realized_pnl', 0.0) # Assuming this is daily from broker
        # If 'realized_pnl' is lifetime, daily P&L needs to be tracked from start of day.
        # For SimBroker, 'realized_pnl' is total. Daily P&L tracking needs more state.
        # Let's assume for now it's "today's realized P&L". This is a simplification.
        logger.debug(f"RiskManager updated daily P&L: {self.current_daily_pnl}")

    def check_daily_loss_limit(self) -> bool:
        """Returns True if daily loss limit has been breached."""
        if self.daily_loss_limit_usd is None:
            return False # No limit set
        breached = self.current_daily_pnl <= -abs(self.daily_loss_limit_usd)
        if breached:
            logger.warning(f"DAILY LOSS LIMIT BREACHED! Current P&L: ${self.current_daily_pnl:.2f}, Limit: ${-abs(self.daily_loss_limit_usd):.2f}")
        return breached

    def check_max_position_size(self, current_position_qty: float, order_qty: float) -> bool:
        """
        Checks if placing an order of order_qty would exceed max_position_size.
        Assumes current_position_qty is signed (positive for long, negative for short).
        Order_qty is unsigned. Side of order needs to be known if current_position_qty is 0.
        More accurately, this should check against the total intended position size.
        """
        # This simple check is for total contracts regardless of instrument if RiskManager is global.
        # If per instrument, then current_position_qty should be for that specific instrument.
        # For now, assume it's for the primary configured instrument.

        # If current_position_qty is 0, then proposed_qty is just order_qty.
        # If current_position_qty is non-zero, and order is same direction, proposed = current + order.
        # If current_position_qty is non-zero, and order is opposite, proposed = abs(current - order) if reducing,
        # or order_qty if flipping and new_qty > current_qty. This is complex.

        # Let's simplify: the check is on the absolute quantity of the position that *would result*.
        # The caller (strategy logic) should determine the intended final position size.
        # This function just checks if a given `potential_total_qty` is too large.

        # Alternative: check based on current position + new order quantity
        # This needs order_side. Assume this function is called BEFORE placing an order.
        # Let `current_abs_qty = abs(current_position_qty)`
        # If new order adds to position: `potential_abs_qty = current_abs_qty + order_qty`
        # If new order reduces/flips: this check might not be the one to use, or needs more context.

        # The most straightforward is that the strategy proposes a trade (e.g., "go long 2 lots")
        # and this function checks if "2 lots" > max_position_size.
        # Or, if current pos is +1 lot, and strategy wants to "go long 1 more lot", then check 1+1 vs max.

        if self.max_position_size is None:
            return False # No limit

        # This is the quantity of the trade itself, not the total position.
        # The pattern detector should decide entry size, and this checks that single trade size.
        # The overall position size should be checked by the strategy before calling this for a new entry.
        # For now, let's assume `order_qty` is the size of the trade being considered.
        if order_qty > self.max_position_size:
             logger.warning(f"Order quantity {order_qty} exceeds max single trade size {self.max_position_size} (interpreted as max_position_size).")
             return True # Breached if order_qty itself is > max_position_size (config name is ambiguous)

        # More robust: Check total position AFTER the trade
        # This requires knowing the side of the new order and current position.
        # Let's assume the strategy provides the *target* position size for the check.
        # `check_target_position_size(target_qty: float)`
        # For now, the current interpretation is simpler: is the order itself too big?
        # This might be better named `max_order_size` in config.

        # If `max_position_size` truly means the total exposure:
        # current_pos_val = current_position_qty # signed
        # order_val = order_qty if order_side == BUY else -order_qty
        # future_pos_val = current_pos_val + order_val
        # if abs(future_pos_val) > self.max_position_size: return True
        # This seems more correct for "max_position_size".
        # The current method signature is a bit ambiguous.
        # Let's assume the caller is checking `order_qty` against a limit on single order size for now.
        # And separately, strategy checks total position.

        return False # Placeholder - this needs clarification on what max_position_size means.

    def is_risk_off_active(self, current_pos_qty: float, order_qty_to_add: float) -> bool:
        """Checks all hard risk-off conditions."""
        if self.check_daily_loss_limit():
            logger.critical("RISK-OFF: Daily loss limit reached.")
            return True

        # This is where a more robust max total position check should live
        # Example: if abs(current_pos_qty + order_qty_to_add_signed) > self.max_position_size:
        #   logger.critical(f"RISK-OFF: Order would exceed max total position size.")
        #   return True

        # Add trailing drawdown check here when implemented
        return False

    def calculate_initial_stops_and_targets(self,
                                            entry_price: float,
                                            side: OrderSide,
                                            atr: float,
                                            accumulation_low: float,
                                            accumulation_high: float,
                                            spotgamma_levels: Optional[Dict] = None
                                           ) -> Dict[str, Optional[float]]:
        """
        Calculates initial Stop Loss, Take Profit 1, and Take Profit 2.
        'accumulation_low' and 'accumulation_high' define the zone used for stop placement.
        'spotgamma_levels' can provide alternative TP2 levels (e.g. "Combo Strike").
        """
        if atr <= 0:
            logger.warning("ATR is zero or negative, cannot calculate stops/targets based on ATR.")
            return {"stop_loss": None, "take_profit_1": None, "take_profit_2": None}

        stop_loss_price = None
        tp1_price = None
        tp2_price = None

        stop_buffer_amount = self.stop_buffer_ticks * self.tick_size

        if side == OrderSide.BUY:
            # Stop is below accumulation low
            stop_loss_price = accumulation_low - stop_buffer_amount
            tp1_price = entry_price + (self.target1_atr_multiplier * atr)
            tp2_price = entry_price + (self.target2_atr_multiplier * atr)

            # TODO: Incorporate SpotGamma "Combo Strike" for TP2 if applicable
            # if self.use_spotgamma_tp2 and spotgamma_levels and 'combo_strikes' in spotgamma_levels:
            #     # Find nearest combo strike above entry that could be TP2
            #     pass

        elif side == OrderSide.SELL:
            # Stop is above accumulation high
            stop_loss_price = accumulation_high + stop_buffer_amount
            tp1_price = entry_price - (self.target1_atr_multiplier * atr)
            tp2_price = entry_price - (self.target2_atr_multiplier * atr)

            # TODO: Incorporate SpotGamma "Combo Strike" for TP2
            # if self.use_spotgamma_tp2 and spotgamma_levels and 'combo_strikes' in spotgamma_levels:
            #     # Find nearest combo strike below entry
            #     pass

        # Round to nearest tick size (important for actual order placement)
        if stop_loss_price: stop_loss_price = round(stop_loss_price / self.tick_size) * self.tick_size
        if tp1_price: tp1_price = round(tp1_price / self.tick_size) * self.tick_size
        if tp2_price: tp2_price = round(tp2_price / self.tick_size) * self.tick_size

        return {
            "stop_loss": stop_loss_price,
            "take_profit_1": tp1_price,
            "take_profit_2": tp2_price
        }

    def manage_active_trade(self,
                            client_order_id: str, # ID of the entry order for this trade
                            current_price: float,
                            position: Position, # Current position object for this trade/instrument
                            atr: float,
                            initial_stops_targets: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
        """
        Manages an active trade: updates trailing stop if TP1 hit.
        Returns new stop price if it changed, otherwise None or existing stop.
        `initial_stops_targets` is what was calculated at entry.
        This method would be called on each price update for an active trade.
        """
        if atr <= 0: return {} # Cannot manage without valid ATR

        trade_risk_params = self.active_trade_risks.get(client_order_id)
        if not trade_risk_params:
            # First time managing this trade, store its initial params
            trade_risk_params = {
                "initial_stop": initial_stops_targets.get("stop_loss"),
                "current_stop": initial_stops_targets.get("stop_loss"), # Initially same as initial_stop
                "tp1": initial_stops_targets.get("take_profit_1"),
                "tp2": initial_stops_targets.get("take_profit_2"),
                "trailing_active": False,
                "entry_price": position.average_entry_price # Assuming position is for this trade
            }
            self.active_trade_risks[client_order_id] = trade_risk_params
            logger.info(f"RiskManager: Starting to manage trade {client_order_id}. Initial Stop: {trade_risk_params['initial_stop']}, TP1: {trade_risk_params['tp1']}")


        new_stop_to_place = None # This is what the function will return if a new stop order needs action

        # Check if TP1 is hit (if not already trailing)
        if not trade_risk_params["trailing_active"] and trade_risk_params["tp1"] is not None:
            if position.quantity > 0: # Long trade
                if current_price >= trade_risk_params["tp1"]:
                    logger.info(f"Trade {client_order_id}: TP1 hit at {current_price} (TP1 level: {trade_risk_params['tp1']}). Activating trail.")
                    trade_risk_params["trailing_active"] = True
            elif position.quantity < 0: # Short trade
                if current_price <= trade_risk_params["tp1"]:
                    logger.info(f"Trade {client_order_id}: TP1 hit at {current_price} (TP1 level: {trade_risk_params['tp1']}). Activating trail.")
                    trade_risk_params["trailing_active"] = True

        # If trailing is active, calculate new trail stop
        if trade_risk_params["trailing_active"]:
            trail_amount = self.trail_atr_multiplier * atr
            proposed_trail_stop = None
            if position.quantity > 0: # Long trade, trail stop moves up
                proposed_trail_stop = current_price - trail_amount
                # Ensure trail stop only moves up, not down
                if trade_risk_params["current_stop"] is None or proposed_trail_stop > trade_risk_params["current_stop"]:
                    new_stop_to_place = round(proposed_trail_stop / self.tick_size) * self.tick_size
            elif position.quantity < 0: # Short trade, trail stop moves down
                proposed_trail_stop = current_price + trail_amount
                if trade_risk_params["current_stop"] is None or proposed_trail_stop < trade_risk_params["current_stop"]:
                    new_stop_to_place = round(proposed_trail_stop / self.tick_size) * self.tick_size

            if new_stop_to_place is not None and new_stop_to_place != trade_risk_params["current_stop"]:
                logger.info(f"Trade {client_order_id}: Trailing stop updated. Old: {trade_risk_params['current_stop']}, New: {new_stop_to_place} (Current Price: {current_price}, ATR: {atr:.2f})")
                trade_risk_params["current_stop"] = new_stop_to_place
                # The calling logic will use this new_stop_to_place to modify the actual stop order

        # Return the stop that should be active (either initial or new trail)
        return {"new_stop_price": trade_risk_params["current_stop"]}


    def on_trade_closed(self, client_order_id: str):
        """Cleans up risk parameters for a closed trade."""
        if client_order_id in self.active_trade_risks:
            del self.active_trade_risks[client_order_id]
            logger.info(f"RiskManager: Removed active risk parameters for closed trade {client_order_id}.")

    def get_active_stop_price(self, client_order_id: str) -> Optional[float]:
        params = self.active_trade_risks.get(client_order_id)
        return params["current_stop"] if params else None


if __name__ == '__main__': # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    class DummyConfigRisk:
        def get(self, key, default=None):
            cfg = {
                'trading.instrument': 'MNQ',
                'instruments.MNQ.tick_size': 0.25,
                'trading.target_atr_multiplier': 1.0,
                'trading.target2_atr_multiplier': 2.0,
                'trading.stop_buffer_ticks': 4, # 1 point for MNQ
                'trading.trail_atr_multiplier': 1.5,
                'trading.max_position_size': 2,
                'risk_management.daily_loss_limit_usd': 500.0,
            }
            # Basic dot notation access
            keys = key.split('.')
            val = cfg
            try:
                for k_part in keys: val = val[k_part]
                return val
            except KeyError: return default
            except TypeError: return default # if a key part is not a dict

    rm = RiskManager(config=DummyConfigRisk())

    # Test Daily Loss Limit
    rm.update_account_summary({"realized_pnl": -600})
    assert rm.check_daily_loss_limit() is True
    logger.info(f"Daily loss limit check (P&L -600): {'Breached' if rm.check_daily_loss_limit() else 'OK'}")
    rm.update_account_summary({"realized_pnl": -400})
    assert rm.check_daily_loss_limit() is False
    logger.info(f"Daily loss limit check (P&L -400): {'Breached' if rm.check_daily_loss_limit() else 'OK'}")

    # Test Stops and Targets Calculation
    entry_long = 15000
    atr_val = 10.0 # 10 points ATR
    acc_low = 14990
    acc_high = 14998 # Accumulation zone for a long entry would be *below* entry

    # For a BUY from 15000, accumulation should be below it. Let's say:
    # Accumulation during a bullish imbalance ending, price pulls back to its top edge.
    # So entry is near acc_high.
    entry_price_long = 14998.0
    acc_low_long = 14990.0
    acc_high_long = 14998.0 # Entry at top of accumulation after bullish imbalance

    levels_long = rm.calculate_initial_stops_and_targets(
        entry_price=entry_price_long, side=OrderSide.BUY, atr=atr_val,
        accumulation_low=acc_low_long, accumulation_high=acc_high_long
    )
    logger.info(f"Long trade from {entry_price_long} (Acc Low: {acc_low_long}): {levels_long}")
    # Expected SL: 14990 (acc_low) - 1 (4 ticks buffer) = 14989
    # Expected TP1: 14998 + 1*10 = 15008
    # Expected TP2: 14998 + 2*10 = 15018
    assert levels_long["stop_loss"] == (acc_low_long - rm.stop_buffer_ticks * rm.tick_size)
    assert levels_long["take_profit_1"] == entry_price_long + rm.target1_atr_multiplier * atr_val

    entry_price_short = 15050.0
    acc_low_short = 15050.0 # Entry at bottom of accumulation after bearish imbalance
    acc_high_short = 15058.0
    levels_short = rm.calculate_initial_stops_and_targets(
        entry_price=entry_price_short, side=OrderSide.SELL, atr=atr_val,
        accumulation_low=acc_low_short, accumulation_high=acc_high_short
    )
    logger.info(f"Short trade from {entry_price_short} (Acc High: {acc_high_short}): {levels_short}")
    # Expected SL: 15058 (acc_high) + 1 (4 ticks buffer) = 15059
    # Expected TP1: 15050 - 1*10 = 15040
    assert levels_short["stop_loss"] == (acc_high_short + rm.stop_buffer_ticks * rm.tick_size)
    assert levels_short["take_profit_1"] == entry_price_short - rm.target1_atr_multiplier * atr_val

    # Test Trailing Stop Management
    mock_pos_long = Position(instrument="MNQ", quantity=1, average_entry_price=entry_price_long)
    trade_id_long = "test_long_trade_01"

    # Initial management call (registers the trade)
    rm.manage_active_trade(trade_id_long, current_price=entry_price_long, position=mock_pos_long, atr=atr_val, initial_stops_targets=levels_long)
    assert rm.active_trade_risks[trade_id_long]["trailing_active"] is False
    assert rm.active_trade_risks[trade_id_long]["current_stop"] == levels_long["stop_loss"]

    # Price moves, but not past TP1
    price_moves_1 = entry_price_long + 0.5 * atr_val # 15003
    stop_update = rm.manage_active_trade(trade_id_long, current_price=price_moves_1, position=mock_pos_long, atr=atr_val, initial_stops_targets=levels_long)
    assert rm.active_trade_risks[trade_id_long]["trailing_active"] is False
    assert stop_update.get("new_stop_price") == levels_long["stop_loss"] # Stop hasn't changed

    # Price hits TP1
    price_hits_tp1 = levels_long["take_profit_1"] # 15008
    stop_update = rm.manage_active_trade(trade_id_long, current_price=price_hits_tp1, position=mock_pos_long, atr=atr_val, initial_stops_targets=levels_long)
    assert rm.active_trade_risks[trade_id_long]["trailing_active"] is True
    expected_trail_stop_at_tp1 = price_hits_tp1 - (rm.trail_atr_multiplier * atr_val) # 15008 - 1.5*10 = 14993
    expected_trail_stop_at_tp1_rounded = round(expected_trail_stop_at_tp1 / rm.tick_size) * rm.tick_size
    assert stop_update.get("new_stop_price") == expected_trail_stop_at_tp1_rounded
    logger.info(f"Trail activated. Current price {price_hits_tp1}, new stop {stop_update.get('new_stop_price')}")

    # Price moves further up, trail stop should follow
    price_moves_2 = price_hits_tp1 + 0.5 * atr_val # 15008 + 5 = 15013
    stop_update = rm.manage_active_trade(trade_id_long, current_price=price_moves_2, position=mock_pos_long, atr=atr_val, initial_stops_targets=levels_long)
    expected_trail_stop_2 = price_moves_2 - (rm.trail_atr_multiplier * atr_val) # 15013 - 15 = 14998
    expected_trail_stop_2_rounded = round(expected_trail_stop_2 / rm.tick_size) * rm.tick_size
    assert stop_update.get("new_stop_price") == expected_trail_stop_2_rounded
    logger.info(f"Price moves to {price_moves_2}, trail stop updated to {stop_update.get('new_stop_price')}")

    # Price retraces, trail stop should NOT move down
    price_moves_3 = price_moves_2 - 0.2 * atr_val # 15013 - 2 = 15011
    current_trail_stop_before_retrace = rm.active_trade_risks[trade_id_long]["current_stop"]
    stop_update = rm.manage_active_trade(trade_id_long, current_price=price_moves_3, position=mock_pos_long, atr=atr_val, initial_stops_targets=levels_long)
    assert stop_update.get("new_stop_price") == current_trail_stop_before_retrace # Stop should not have moved down
    logger.info(f"Price retraces to {price_moves_3}, trail stop remains {stop_update.get('new_stop_price')}")

    rm.on_trade_closed(trade_id_long)
    assert trade_id_long not in rm.active_trade_risks

    logger.info("RiskManager tests completed.")
