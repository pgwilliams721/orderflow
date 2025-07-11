import asyncio
import time
import logging
import uuid
import csv # For CSV logging
from pathlib import Path # For CSV file path handling
from typing import List, Dict, Any, Optional, Callable

from delta_rollover_bot.config_loader import Config
from delta_rollover_bot.trading.broker_interface import (
    BrokerInterface, Order, OrderSide, OrderType, OrderStatus, Position, Fill
)
from delta_rollover_bot.data.data_provider_interface import DataEvent # For market data updates

logger = logging.getLogger(__name__)

DEFAULT_TRADES_CSV_DIR = Path("./trades_history") # Store trade logs in a dedicated directory

class SimBroker(BrokerInterface):
    """
    Simulated broker for backtesting.
    It processes orders based on incoming market data events and logs fills to CSV.
    """
    def __init__(self, config: Config, event_loop: asyncio.AbstractEventLoop):
        super().__init__(config, event_loop)
        self.instrument_configs = self.config.get("instruments", {})

        # Internal state
        self._open_orders: Dict[str, Order] = {} # client_order_id -> Order
        self._positions: Dict[str, Position] = {} # instrument -> Position
        self._account_balance: float = self.config.get("backtesting.initial_capital", 100000.0)
        self._realized_pnl_total: float = 0.0
        self._last_market_prices: Dict[str, float] = {} # instrument -> last known price

        self.trade_syncer_hook: Optional[Callable[[Fill], None]] = None # For TradeSyncer

        # CSV Logging for Fills
        self.trades_csv_path_str = self.config.get("logging.trades_csv_file", "sim_trades_log.csv")
        self.trades_csv_file_path: Optional[Path] = None
        self._csv_writer = None
        self._csv_file_handle = None
        self._setup_trades_csv()

        logger.info(f"SimBroker initialized with initial capital: {self._account_balance}. Trades CSV: {self.trades_csv_file_path}")

    def _setup_trades_csv(self):
        """Initializes the CSV file and writer for logging fills."""
        if not self.trades_csv_path_str:
            logger.warning("SimBroker: Trades CSV file path not configured. CSV logging disabled.")
            return

        trades_csv_p = Path(self.trades_csv_path_str)
        if trades_csv_p.is_absolute():
            log_dir = trades_csv_p.parent
            self.trades_csv_file_path = trades_csv_p
        else:
            log_dir = DEFAULT_TRADES_CSV_DIR
            self.trades_csv_file_path = log_dir / trades_csv_p

        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            # Open file in append mode, create if not exists. `newline=''` is important for csv.
            self._csv_file_handle = open(self.trades_csv_file_path, mode='a', newline='', encoding='utf-8')
            self._csv_writer = csv.writer(self._csv_file_handle)

            # Write header if the file is new/empty
            self._csv_file_handle.seek(0, 2) # Go to end of file
            if self._csv_file_handle.tell() == 0: # File is empty
                header = [
                    "fill_timestamp_ns", "fill_datetime_utc", "instrument", "side",
                    "quantity", "price", "commission", "fee",
                    "order_id", "client_order_id", "fill_id"
                ]
                self._csv_writer.writerow(header)
                self._csv_file_handle.flush() # Ensure header is written immediately
        except IOError as e:
            logger.error(f"SimBroker: Failed to open or write header to trades CSV file {self.trades_csv_file_path}: {e}")
            if self._csv_file_handle:
                self._csv_file_handle.close()
            self._csv_file_handle = None
            self._csv_writer = None
            self.trades_csv_file_path = None # Disable if setup fails

    async def connect(self):
        logger.info("SimBroker: 'Connected' (simulation mode).")
        # No actual connection needed for SimBroker
        pass

    async def disconnect(self):
        logger.info("SimBroker: 'Disconnected' (simulation mode).")
        if self._csv_file_handle:
            try:
                self._csv_file_handle.close()
                logger.info(f"SimBroker: Trades CSV file {self.trades_csv_file_path} closed.")
            except IOError as e:
                logger.error(f"SimBroker: Error closing trades CSV file: {e}")
            self._csv_file_handle = None
            self._csv_writer = None
        pass

    def _generate_broker_order_id(self) -> str:
        return f"sim_{uuid.uuid4().hex[:12]}"

    async def place_order(self, order: Order) -> Order:
        if not order.client_order_id: # Should be set by Order class default
            order.client_order_id = f"sim_co_{uuid.uuid4().hex[:8]}"

        if order.client_order_id in self._open_orders:
            order.status = OrderStatus.REJECTED
            order.message = "Duplicate client_order_id"
            logger.warning(f"SimBroker: Order rejected, duplicate client_order_id: {order.client_order_id}")
            await self._publish_order_update(order)
            return order

        order.order_id = self._generate_broker_order_id()
        order.status = OrderStatus.NEW # Assume accepted by default in sim if not immediately filled
        order.timestamp_ns = int(time.time() * 1e9)

        self._open_orders[order.client_order_id] = order
        logger.info(f"SimBroker: Order placed and accepted (NEW): {order}")
        await self._publish_order_update(order)

        # For market orders, try to fill immediately if price is known
        if order.order_type == OrderType.MARKET:
            if order.instrument in self._last_market_prices:
                # In simulation, market orders fill at the current known price
                # We might add slippage simulation later
                await self._try_fill_order(order, self._last_market_prices[order.instrument])
            else:
                logger.warning(f"SimBroker: Market order {order.client_order_id} for {order.instrument} "
                               f"cannot fill immediately, no price data yet. Will fill on next tick.")

        return order

    async def cancel_order(self, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Optional[Order]:
        target_order: Optional[Order] = None
        search_key = None

        if client_order_id and client_order_id in self._open_orders:
            target_order = self._open_orders[client_order_id]
            search_key = client_order_id
        elif order_id:
            for co_id, o in self._open_orders.items():
                if o.order_id == order_id:
                    target_order = o
                    search_key = co_id
                    break

        if target_order:
            if target_order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
                logger.warning(f"SimBroker: Cannot cancel order {target_order.client_order_id}, already in terminal state: {target_order.status.name}")
                # Return current status, no change
                return target_order

            original_status = target_order.status
            target_order.status = OrderStatus.CANCELLED
            target_order.message = "Cancelled by user."
            target_order.timestamp_ns = int(time.time() * 1e9)

            if search_key and search_key in self._open_orders: # search_key is client_order_id here
                 del self._open_orders[search_key]

            logger.info(f"SimBroker: Order cancelled: {target_order}")
            await self._publish_order_update(target_order)
            return target_order
        else:
            logger.warning(f"SimBroker: Cancel failed, order not found. OrderID: {order_id}, ClientOrderID: {client_order_id}")
            # To conform to return type, we might need to construct a dummy "not found" order or handle differently
            # For now, returning None if not found by ID. If an Order object is always expected, this needs adjustment.
            # The interface implies returning an Order. Let's create a rejected one if not found.
            # This is debatable. For now, let's stick to returning None if not found.
            return None


    async def modify_order(self, order_id: Optional[str] = None,
                           new_quantity: Optional[float] = None,
                           new_limit_price: Optional[float] = None,
                           new_stop_price: Optional[float] = None,
                           client_order_id: Optional[str] = None) -> Optional[Order]:
        # SimBroker: Simplistic modify = cancel + replace.
        # More sophisticated simulation might handle modifications differently.
        logger.info(f"SimBroker: Modifying order (ClientID: {client_order_id}, BrokerID: {order_id}). "
                    f"New Qty: {new_quantity}, New Lmt: {new_limit_price}, New Stp: {new_stop_price}")

        order_to_modify: Optional[Order] = None
        search_client_id = None

        if client_order_id and client_order_id in self._open_orders:
            order_to_modify = self._open_orders[client_order_id]
            search_client_id = client_order_id
        elif order_id:
            for co_id, o in self._open_orders.items():
                if o.order_id == order_id:
                    order_to_modify = o
                    search_client_id = co_id
                    break

        if not order_to_modify or not search_client_id:
            logger.warning(f"SimBroker: Modify failed, order not found. OrderID: {order_id}, ClientOrderID: {client_order_id}")
            return None # Or a rejected order object

        if order_to_modify.status in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]:
            logger.warning(f"SimBroker: Cannot modify order {order_to_modify.client_order_id}, already in terminal state: {order_to_modify.status.name}")
            return order_to_modify

        # Create a new order with the modified parameters but same client_order_id (for user tracking)
        # or generate a new client_order_id if broker policy is strict replacement.
        # For sim, let's assume we can "modify" it by creating a new order object based on the old one.

        cancelled_order = await self.cancel_order(client_order_id=search_client_id)
        if not cancelled_order or cancelled_order.status != OrderStatus.CANCELLED:
            logger.error(f"SimBroker: Failed to cancel original order {search_client_id} during modify. Aborting modify.")
            # Potentially revert cancel if possible, or return the original order with its current state.
            return order_to_modify

        # Create new order based on the old one, applying changes
        # Important: The new order should ideally get a new broker order_id.
        modified_order_params = {
            "instrument": order_to_modify.instrument,
            "side": order_to_modify.side,
            "order_type": order_to_modify.order_type, # Type usually not modifiable, but price/qty
            "quantity": new_quantity if new_quantity is not None else order_to_modify.quantity,
            "limit_price": new_limit_price if new_limit_price is not None else order_to_modify.limit_price,
            "stop_price": new_stop_price if new_stop_price is not None else order_to_modify.stop_price,
            "client_order_id": order_to_modify.client_order_id # Keep same client ID for user tracking
        }

        # Validate new parameters (e.g. limit price for limit order)
        if modified_order_params["order_type"] == OrderType.LIMIT and modified_order_params["limit_price"] is None:
            logger.error("SimBroker: Modify failed. Limit order must have a limit price.")
            # This should result in a rejection for the new order part.
            # For simplicity, we don't "un-cancel" the old one here. User needs to resubmit.
            return cancelled_order # Return the cancelled order

        new_order_request = Order(**modified_order_params)

        # Place the "new" (modified) order
        placed_modified_order = await self.place_order(new_order_request)
        logger.info(f"SimBroker: Order {search_client_id} modified (Cancel/Replace). New order: {placed_modified_order.order_id}")

        return placed_modified_order


    async def get_order_status(self, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Optional[Order]:
        if client_order_id and client_order_id in self._open_orders:
            return self._open_orders[client_order_id]
        elif order_id:
            for order in self._open_orders.values():
                if order.order_id == order_id:
                    return order
        # Might need to check historical/filled orders too if not just "open"
        logger.warning(f"SimBroker: get_order_status - Order not found (OrderID: {order_id}, ClientOID: {client_order_id})")
        return None

    async def get_open_orders(self, instrument: Optional[str] = None) -> List[Order]:
        if instrument:
            return [o for o in self._open_orders.values() if o.instrument == instrument and o.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]]
        return [o for o in self._open_orders.values() if o.status not in [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]]

    async def get_positions(self, instrument: Optional[str] = None) -> List[Position]:
        # Update Unrealized P&L for positions based on last market price
        for inst, pos in self._positions.items():
            if inst in self._last_market_prices:
                price_diff = self._last_market_prices[inst] - pos.average_entry_price
                # Get point value for P&L calculation
                instr_conf = self.instrument_configs.get(inst, {})
                point_value = instr_conf.get("point_value", 1) # Default to 1 if not configured
                tick_size = instr_conf.get("tick_size", 0.01)

                # P&L per contract is price_diff. Total P&L is price_diff * quantity.
                # For futures, P&L = (Exit Price - Entry Price) * Quantity * Point Value / Tick Size (if price_diff is in points)
                # Or more simply: (Exit Price - Entry Price) * Quantity * Multiplier
                # If average_entry_price and _last_market_prices[inst] are actual prices,
                # then P&L = (current_price - entry_price) * quantity * (point_value / tick_size_for_1_point)
                # Assuming point_value is "value of 1 full point move", and price_diff is in points.
                pos.unrealized_pnl = price_diff * pos.quantity * point_value
            else:
                pos.unrealized_pnl = 0 # Or None if price unknown

        if instrument:
            if instrument in self._positions:
                return [self._positions[instrument]]
            return []
        return list(self._positions.values())

    async def get_account_summary(self) -> Dict[str, Any]:
        total_unrealized_pnl = sum(pos.unrealized_pnl for pos in self._positions.values() if pos.unrealized_pnl is not None)
        equity = self._account_balance + self._realized_pnl_total + total_unrealized_pnl

        summary = {
            "account_balance": self._account_balance,
            "realized_pnl": self._realized_pnl_total,
            "unrealized_pnl": total_unrealized_pnl,
            "equity": equity,
            "margin_available": equity, # Simplistic: all equity is available margin in sim
            "margin_used": 0.0, # Simplistic
            "num_open_orders": len(await self.get_open_orders()),
            "num_positions": len(self._positions)
        }
        await self._publish_account_summary_update(summary)
        return summary

    async def on_market_data(self, event: DataEvent):
        """
        Process incoming market data (e.g., from BookmapConnector) to update prices
        and attempt to fill orders.
        Expected event.data: {"instrument": "MNQ", "best_bid": ..., "best_ask": ...}
        or {"instrument": "MNQ", "price": ...} (last trade price)
        """
        if not (event.data and "instrument" in event.data):
            return

        instrument = event.data["instrument"]

        # Determine price for filling. Use midpoint or last trade.
        # This logic might need to be more robust depending on event structure.
        current_price_for_fills = None
        if "best_bid" in event.data and "best_ask" in event.data:
            best_bid = float(event.data["best_bid"])
            best_ask = float(event.data["best_ask"])
            self._last_market_prices[instrument] = (best_bid + best_ask) / 2.0 # Store mid as general last price
            # For filling, use bid for sells, ask for buys to simulate crossing the spread
            # This is a basic way to simulate fills.
        elif "price" in event.data: # If it's a trade event with a single price
            current_price_for_fills = float(event.data["price"])
            self._last_market_prices[instrument] = current_price_for_fills
        else:
            # logger.debug(f"SimBroker: No usable price in market data event for {instrument}: {event.data}")
            return # No price to act upon

        # Try to fill any relevant open orders
        # Iterate over a copy of items in case of modification during iteration
        for client_oid, order in list(self._open_orders.items()):
            if order.instrument == instrument and order.status == OrderStatus.NEW:
                fill_price_for_this_order = None
                if order.order_type == OrderType.MARKET:
                    # For market orders, use the appropriate side of BBO if available, else last trade price
                    if "best_bid" in event.data and "best_ask" in event.data:
                        fill_price_for_this_order = float(event.data["best_ask"]) if order.side == OrderSide.BUY else float(event.data["best_bid"])
                    else: # Fallback to general current_price_for_fills (e.g. last trade)
                        fill_price_for_this_order = current_price_for_fills

                elif order.order_type == OrderType.LIMIT:
                    if "best_bid" in event.data and "best_ask" in event.data:
                        # Limit Buy: fill if market ask <= limit price
                        if order.side == OrderSide.BUY and float(event.data["best_ask"]) <= order.limit_price:
                            fill_price_for_this_order = min(float(event.data["best_ask"]), order.limit_price) # Fill at market or limit, whichever is better for buyer
                        # Limit Sell: fill if market bid >= limit price
                        elif order.side == OrderSide.SELL and float(event.data["best_bid"]) >= order.limit_price:
                            fill_price_for_this_order = max(float(event.data["best_bid"]), order.limit_price) # Fill at market or limit, whichever is better for seller
                    # If only last trade price is available, check against that (less realistic for limits)
                    elif current_price_for_fills is not None:
                         if order.side == OrderSide.BUY and current_price_for_fills <= order.limit_price:
                            fill_price_for_this_order = current_price_for_fills
                         elif order.side == OrderSide.SELL and current_price_for_fills >= order.limit_price:
                            fill_price_for_this_order = current_price_for_fills

                elif order.order_type == OrderType.STOP_MARKET:
                     # Stop Buy: trigger if market ask >= stop price
                    if "best_ask" in event.data and order.side == OrderSide.BUY and float(event.data["best_ask"]) >= order.stop_price:
                        logger.info(f"SimBroker: Stop Buy order {order.client_order_id} triggered for {instrument} at market ask {event.data['best_ask']} (stop price {order.stop_price})")
                        order.order_type = OrderType.MARKET # Convert to market order
                        order.message = f"Stop triggered at {event.data['best_ask']}"
                        # Now it will be filled as a market order in the next pass or immediately if possible
                        fill_price_for_this_order = float(event.data["best_ask"]) # Fill at trigger price (or slightly worse)
                    # Stop Sell: trigger if market bid <= stop price
                    elif "best_bid" in event.data and order.side == OrderSide.SELL and float(event.data["best_bid"]) <= order.stop_price:
                        logger.info(f"SimBroker: Stop Sell order {order.client_order_id} triggered for {instrument} at market bid {event.data['best_bid']} (stop price {order.stop_price})")
                        order.order_type = OrderType.MARKET
                        order.message = f"Stop triggered at {event.data['best_bid']}"
                        fill_price_for_this_order = float(event.data["best_bid"])
                    # If only last trade price is available for stop checks
                    elif current_price_for_fills is not None:
                        if order.side == OrderSide.BUY and current_price_for_fills >= order.stop_price:
                            order.order_type = OrderType.MARKET; fill_price_for_this_order = current_price_for_fills
                        elif order.side == OrderSide.SELL and current_price_for_fills <= order.stop_price:
                            order.order_type = OrderType.MARKET; fill_price_for_this_order = current_price_for_fills

                # If a fill price was determined, attempt to fill
                if fill_price_for_this_order is not None:
                    await self._try_fill_order(order, fill_price_for_this_order)

        # After processing fills, update positions' P&L (done in get_positions, but can force here too)
        await self.get_account_summary() # This will update and publish summary including P&L

    async def _try_fill_order(self, order: Order, fill_price: float):
        # In SimBroker, assume full fill if conditions met
        order.status = OrderStatus.FILLED
        order.avg_fill_price = fill_price
        order.filled_quantity = order.quantity
        order.timestamp_ns = int(time.time() * 1e9)

        fill_event = Fill(
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            fill_id=f"sim_fill_{uuid.uuid4().hex[:10]}",
            instrument=order.instrument,
            side=order.side,
            price=fill_price,
            quantity=order.quantity,
            timestamp_ns=order.timestamp_ns,
            # Simulate basic commission/fees if needed from config
            commission=self.config.get(f"instruments.{order.instrument}.commission_per_contract", 0.0) * order.quantity
        )

        logger.info(f"SimBroker: Order FILLED: {order}. Fill: {fill_event}")

        # Update position
        self._update_position(fill_event)

        # Publish updates
        await self._publish_order_update(order)
        await self._publish_fill_update(fill_event) # This also logs to CSV internally now

        # Log fill to CSV
        self._log_fill_to_csv(fill_event)

        # Call TradeSyncer hook if registered
        if self.trade_syncer_hook:
            try:
                self.trade_syncer_hook(fill_event) # This hook should be non-async or wrapped
            except Exception as e:
                logger.error(f"SimBroker: Error calling TradeSyncer hook: {e}")

        # Remove from open orders
        if order.client_order_id in self._open_orders:
            del self._open_orders[order.client_order_id]

    def _log_fill_to_csv(self, fill: Fill):
        """Writes a fill event to the CSV log file."""
        if not self._csv_writer or not self._csv_file_handle:
            logger.debug("SimBroker: CSV writer not available, skipping CSV log for fill.")
            return

        try:
            # Convert ns timestamp to human-readable UTC datetime string
            fill_dt_utc = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(fill.timestamp_ns / 1e9))

            row = [
                fill.timestamp_ns,
                fill_dt_utc,
                fill.instrument,
                fill.side.name,
                fill.quantity,
                fill.price,
                fill.commission if fill.commission is not None else 0.0,
                fill.fee if fill.fee is not None else 0.0,
                fill.order_id,
                fill.client_order_id,
                fill.fill_id
            ]
            self._csv_writer.writerow(row)
            self._csv_file_handle.flush() # Ensure it's written to disk
        except Exception as e:
            logger.error(f"SimBroker: Failed to write fill to CSV {self.trades_csv_file_path}: {e}", exc_info=True)


    def _update_position(self, fill: Fill):
        instrument_cfg = self.instrument_configs.get(fill.instrument, {})
        point_value = instrument_cfg.get("point_value", 1.0) # Value of 1 full point

        current_pos = self._positions.get(fill.instrument)

        if current_pos:
            old_qty = current_pos.quantity
            old_avg_px = current_pos.average_entry_price

            trade_qty_signed = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity
            new_qty = old_qty + trade_qty_signed

            if new_qty == 0: # Position closed
                realized_pnl_trade = (fill.price - old_avg_px) * (-old_qty) * point_value # P&L is on the closing part
                self._realized_pnl_total += realized_pnl_trade
                self._account_balance += realized_pnl_trade # Realized P&L directly impacts balance
                del self._positions[fill.instrument]
                logger.info(f"SimBroker: Position closed for {fill.instrument}. Trade P&L: {realized_pnl_trade:.2f}. Total Realized P&L: {self._realized_pnl_total:.2f}")
            elif old_qty * new_qty < 0: # Position flipped (e.g. long to short)
                # Realize P&L on the part that closed the old position
                realized_pnl_flip = (fill.price - old_avg_px) * (-old_qty) * point_value
                self._realized_pnl_total += realized_pnl_flip
                self._account_balance += realized_pnl_flip

                # New position is the remainder
                current_pos.quantity = new_qty
                current_pos.average_entry_price = fill.price # Avg price of the new leg
                logger.info(f"SimBroker: Position Flipped for {fill.instrument}. Old part P&L: {realized_pnl_flip:.2f}. New Pos: {current_pos}")
            else: # Position increased or partially decreased (same direction)
                # If decreasing, realize P&L on the decreased part
                if abs(new_qty) < abs(old_qty):
                    qty_reduced = abs(old_qty) - abs(new_qty)
                    pnl_on_reduction = (fill.price - old_avg_px) * qty_reduced * point_value
                    if old_qty < 0: # If was short, then price diff is (old_avg_px - fill.price)
                        pnl_on_reduction = (old_avg_px - fill.price) * qty_reduced * point_value

                    self._realized_pnl_total += pnl_on_reduction
                    self._account_balance += pnl_on_reduction
                    logger.info(f"SimBroker: Position partially closed for {fill.instrument}. Reduced Qty: {qty_reduced}. P&L on reduction: {pnl_on_reduction:.2f}")

                # Update average price: (old_qty * old_avg_px + signed_trade_qty * fill_price) / new_qty
                # This is only for increasing position size. If reducing, avg price doesn't change.
                if abs(new_qty) > abs(old_qty): # Position size increased
                     current_pos.average_entry_price = (old_qty * old_avg_px + trade_qty_signed * fill.price) / new_qty

                current_pos.quantity = new_qty
                logger.info(f"SimBroker: Position updated for {fill.instrument}: {current_pos}")

        else: # New position
            current_pos = Position(
                instrument=fill.instrument,
                quantity=fill.quantity if fill.side == OrderSide.BUY else -fill.quantity,
                average_entry_price=fill.price
            )
            self._positions[fill.instrument] = current_pos
            logger.info(f"SimBroker: New position opened for {fill.instrument}: {current_pos}")

        # Publish position update
        # Need to ensure the position object sent has up-to-date P&L if possible
        pos_to_publish = self._positions.get(fill.instrument)
        if pos_to_publish: # Could have been deleted if closed out
            if fill.instrument in self._last_market_prices: # Try to update UPL
                 price_diff = self._last_market_prices[fill.instrument] - pos_to_publish.average_entry_price
                 pos_to_publish.unrealized_pnl = price_diff * pos_to_publish.quantity * point_value
            asyncio.create_task(self._publish_position_update(pos_to_publish))
        else: # If position was closed, publish a "flat" position update
            flat_pos = Position(instrument=fill.instrument, quantity=0, average_entry_price=0, unrealized_pnl=0)
            # The realized P&L for THIS position is tricky. The summary has total.
            # We might add last_realized_pnl_trade to the position object.
            asyncio.create_task(self._publish_position_update(flat_pos))

        # Account summary also changes due to realized P&L or commissions/fees
        asyncio.create_task(self.get_account_summary())


if __name__ == '__main__': # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    async def run_sim_broker_test():
        class DummyConfig:
            def get(self, key, default=None):
                cfg = {
                    "backtesting.initial_capital": 50000,
                    "instruments": {
                        "MNQ": {"tick_size": 0.25, "point_value": 5.0, "commission_per_contract": 0.50},
                        "ES": {"tick_size": 0.25, "point_value": 50.0, "commission_per_contract": 2.50}
                    }
                }
                # Basic dot notation access for simplicity
                keys = key.split('.')
                val = cfg
                try:
                    for k in keys: val = val[k]
                    return val
                except KeyError: return default

        loop = asyncio.get_event_loop()
        sim_broker = SimBroker(config=DummyConfig(), event_loop=loop)

        async def order_update_handler(order: Order):
            logger.info(f"TEST_CALLBACK - Order Update: {order}")

        async def fill_update_handler(fill: Fill):
            logger.info(f"TEST_CALLBACK - Fill Update: {fill}")

        async def position_update_handler(position: Position):
            logger.info(f"TEST_CALLBACK - Position Update: {position}")

        async def account_summary_handler(summary: Dict[str, Any]):
            logger.info(f"TEST_CALLBACK - Account Summary: {summary}")

        sim_broker.register_order_update_callback(order_update_handler)
        sim_broker.register_fill_update_callback(fill_update_handler)
        sim_broker.register_position_update_callback(position_update_handler)
        sim_broker.register_account_summary_callback(account_summary_handler)

        await sim_broker.connect()

        # --- Test Scenario ---
        # 1. Place a BUY MARKET order for MNQ
        buy_mkt_order = Order(instrument="MNQ", side=OrderSide.BUY, order_type=OrderType.MARKET, quantity=2)
        placed_buy_mkt = await sim_broker.place_order(buy_mkt_order)

        # Simulate market data tick that should fill the market order
        market_tick1 = DataEvent(event_type="market_data_tick", data={"instrument": "MNQ", "best_bid": 15000.00, "best_ask": 15000.25})
        await sim_broker.on_market_data(market_tick1) # Should fill at 15000.25

        # Check position
        mnq_pos = await sim_broker.get_positions("MNQ")
        logger.info(f"MNQ Position after buy: {mnq_pos}")

        # 2. Place a SELL LIMIT order for MNQ at a higher price
        sell_lmt_order = Order(instrument="MNQ", side=OrderSide.SELL, order_type=OrderType.LIMIT, quantity=1, limit_price=15010.00)
        placed_sell_lmt = await sim_broker.place_order(sell_lmt_order)

        # Simulate market data tick below limit - no fill
        market_tick2 = DataEvent(event_type="market_data_tick", data={"instrument": "MNQ", "best_bid": 15005.00, "best_ask": 15005.25})
        await sim_broker.on_market_data(market_tick2)
        logger.info(f"Open orders after tick2: {await sim_broker.get_open_orders('MNQ')}")

        # Simulate market data tick that should fill the limit order
        market_tick3 = DataEvent(event_type="market_data_tick", data={"instrument": "MNQ", "best_bid": 15010.00, "best_ask": 15010.25})
        await sim_broker.on_market_data(market_tick3) # Should fill at 15010.00 (bid >= limit)

        mnq_pos_after_sell = await sim_broker.get_positions("MNQ")
        logger.info(f"MNQ Position after limit sell: {mnq_pos_after_sell}")

        # 3. Place a SELL STOP_MARKET order for MNQ below current position
        # Current position is long 1 MNQ @ ( (2*15000.25) - (1*15010.00) ) / 1 -> not quite, avg price logic needs check for reduction
        # Avg Price of long 2 was 15000.25. Sold 1 at 15010. PnL for that 1: (15010 - 15000.25)*1*5 = 48.75
        # Remaining 1 long contract is still at avg price 15000.25

        sell_stp_order = Order(instrument="MNQ", side=OrderSide.SELL, order_type=OrderType.STOP_MARKET, quantity=1, stop_price=14995.00)
        placed_sell_stp = await sim_broker.place_order(sell_stp_order)

        # Simulate market data tick that should trigger the stop
        market_tick4 = DataEvent(event_type="market_data_tick", data={"instrument": "MNQ", "best_bid": 14994.75, "best_ask": 14995.00})
        await sim_broker.on_market_data(market_tick4) # Should trigger at 14994.75 (bid <= stop_price) and fill

        mnq_pos_after_stop = await sim_broker.get_positions("MNQ") # Should be flat
        logger.info(f"MNQ Position after stop sell (should be flat): {mnq_pos_after_stop}")

        # 4. Test modify order: Place a limit, then modify it
        buy_lmt_modify_test = Order(instrument="ES", side=OrderSide.BUY, order_type=OrderType.LIMIT, quantity=1, limit_price=4000.00)
        placed_buy_lmt_orig = await sim_broker.place_order(buy_lmt_modify_test)
        logger.info(f"Original ES Limit order: {placed_buy_lmt_orig}")

        modified_es_order = await sim_broker.modify_order(client_order_id=placed_buy_lmt_orig.client_order_id, new_limit_price=4001.00, new_quantity=2)
        logger.info(f"Modified ES Limit order result: {modified_es_order}")
        logger.info(f"Open ES orders: {await sim_broker.get_open_orders('ES')}")


        await sim_broker.disconnect()
        final_summary = await sim_broker.get_account_summary()
        logger.info(f"Final Account Summary: {final_summary}")

    asyncio.run(run_sim_broker_test())
