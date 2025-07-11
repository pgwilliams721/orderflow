import asyncio
import logging
from typing import List, Dict, Any, Optional, Type

from delta_rollover_bot.config_loader import Config
from delta_rollover_bot.trading.broker_interface import (
    BrokerInterface, Order, Position, Fill,
    OrderUpdateCallback, FillUpdateCallback, PositionUpdateCallback, AccountSummaryCallback
)
from delta_rollover_bot.trading.sim_broker import SimBroker
# from delta_rollover_bot.trading.tradeovate_broker import TradeovateBroker # To be added later

logger = logging.getLogger(__name__)

class TradeGateway:
    """
    Acts as a facade for different broker implementations.
    The bot interacts with the TradeGateway, which then delegates
    calls to the actual broker instance (e.g., SimBroker, TradeovateBroker).
    """
    def __init__(self, config: Config, event_loop: asyncio.AbstractEventLoop, broker_type: str = "sim"):
        self.config = config
        self.loop = event_loop
        self.broker: BrokerInterface

        if broker_type.lower() == "sim":
            self.broker = SimBroker(config, event_loop)
            logger.info("TradeGateway initialized with SimBroker.")
        # elif broker_type.lower() == "tradeovate":
        #     self.broker = TradeovateBroker(config, event_loop) # Placeholder for future implementation
        #     logger.info("TradeGateway initialized with TradeovateBroker.")
        else:
            logger.error(f"Unsupported broker type: {broker_type}. Defaulting to SimBroker.")
            self.broker = SimBroker(config, event_loop)
            # raise ValueError(f"Unsupported broker type: {broker_type}")


    async def connect(self):
        """Connect to the underlying broker."""
        await self.broker.connect()

    async def disconnect(self):
        """Disconnect from the underlying broker."""
        await self.broker.disconnect()

    async def place_order(self, order: Order) -> Order:
        """Place an order via the underlying broker."""
        return await self.broker.place_order(order)

    async def cancel_order(self, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Optional[Order]:
        """Cancel an order via the underlying broker."""
        return await self.broker.cancel_order(order_id=order_id, client_order_id=client_order_id)

    async def modify_order(self, order_id: Optional[str] = None,
                           new_quantity: Optional[float] = None,
                           new_limit_price: Optional[float] = None,
                           new_stop_price: Optional[float] = None,
                           client_order_id: Optional[str] = None) -> Optional[Order]:
        """Modify an order via the underlying broker."""
        return await self.broker.modify_order(
            order_id=order_id,
            new_quantity=new_quantity,
            new_limit_price=new_limit_price,
            new_stop_price=new_stop_price,
            client_order_id=client_order_id
        )

    async def get_order_status(self, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Optional[Order]:
        """Get order status from the underlying broker."""
        return await self.broker.get_order_status(order_id=order_id, client_order_id=client_order_id)

    async def get_open_orders(self, instrument: Optional[str] = None) -> List[Order]:
        """Get open orders from the underlying broker."""
        return await self.broker.get_open_orders(instrument=instrument)

    async def get_positions(self, instrument: Optional[str] = None) -> List[Position]:
        """Get current positions from the underlying broker."""
        return await self.broker.get_positions(instrument=instrument)

    async def get_account_summary(self) -> Dict[str, Any]:
        """Get account summary from the underlying broker."""
        return await self.broker.get_account_summary()

    # --- Market Data Handling for SimBroker ---
    async def on_market_data(self, event: Any): # event type is DataEvent from data_provider_interface
        """
        Passes market data to the SimBroker if it's the active broker.
        This is specific to SimBroker's need for market data to simulate fills.
        Real brokers handle fills internally based on their own market data feeds.
        """
        if isinstance(self.broker, SimBroker):
            await self.broker.on_market_data(event)
        # For real brokers, this method might not be needed or used differently.

    # --- Callback Registration ---
    def register_order_update_callback(self, callback: OrderUpdateCallback):
        self.broker.register_order_update_callback(callback)

    def register_fill_update_callback(self, callback: FillUpdateCallback):
        self.broker.register_fill_update_callback(callback)

    def register_position_update_callback(self, callback: PositionUpdateCallback):
        self.broker.register_position_update_callback(callback)

    def register_account_summary_callback(self, callback: AccountSummaryCallback):
        self.broker.register_account_summary_callback(callback)

    # --- TradeSyncer Hook ---
    def set_trade_syncer_hook(self, hook: Callable[[Fill], None]):
        """
        Sets a hook to call when a fill occurs, for TradeSyncer integration.
        This is passed down to SimBroker or potentially handled by real brokers.
        """
        if isinstance(self.broker, SimBroker):
            self.broker.trade_syncer_hook = hook
            logger.info("TradeSyncer hook registered with SimBroker.")
        # For TradeovateBroker, this might involve subscribing to its fill events
        # and then calling the hook.
        # elif isinstance(self.broker, TradeovateBroker):
        #     # TradeovateBroker would need internal logic to call this hook on its fills
        #     logger.info("TradeSyncer hook registration would be handled by TradeovateBroker internally.")
        #     pass
        else:
            logger.warning(f"TradeSyncer hook not applicable for broker type: {type(self.broker).__name__}")


if __name__ == '__main__': # pragma: no cover
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    async def main_gateway_test():
        class DummyConfig:
             def get(self, key, default=None):
                cfg = {
                    "backtesting.initial_capital": 75000,
                    "instruments": {
                        "MNQ": {"tick_size": 0.25, "point_value": 5.0, "commission_per_contract": 0.55},
                    }
                }
                keys = key.split('.')
                val = cfg
                try:
                    for k in keys: val = val[k]
                    return val
                except KeyError: return default

        loop = asyncio.get_event_loop()
        # Initialize gateway with SimBroker
        gateway = TradeGateway(config=DummyConfig(), event_loop=loop, broker_type="sim")

        # Dummy callbacks
        async def my_order_handler(order: Order): logger.info(f"GW_CALLBACK - Order: {order}")
        async def my_fill_handler(fill: Fill): logger.info(f"GW_CALLBACK - Fill: {fill}")
        async def my_pos_handler(pos: Position): logger.info(f"GW_CALLBACK - Pos: {pos}")
        async def my_acct_handler(summary: Dict): logger.info(f"GW_CALLBACK - Acct: {summary}")

        gateway.register_order_update_callback(my_order_handler)
        gateway.register_fill_update_callback(my_fill_handler)
        gateway.register_position_update_callback(my_pos_handler)
        gateway.register_account_summary_callback(my_acct_handler)

        def tradesyncer_dummy_hook(fill: Fill):
            logger.info(f"TRADESYNCER_HOOK called with fill: {fill.fill_id} for {fill.instrument}")

        gateway.set_trade_syncer_hook(tradesyncer_dummy_hook)

        await gateway.connect()

        # Test placing an order through the gateway
        test_order = Order(instrument="MNQ", side=OrderSide.BUY, order_type=OrderType.MARKET, quantity=1)
        placed_order = await gateway.place_order(test_order)
        logger.info(f"Gateway placed order: {placed_order}")

        # Simulate market data (will be passed to SimBroker via gateway)
        # Needs DataEvent from data_provider_interface
        from delta_rollover_bot.data.data_provider_interface import DataEvent
        market_event = DataEvent(event_type="sim_market_tick", data={"instrument": "MNQ", "best_bid": 15100, "best_ask": 15100.25})
        await gateway.on_market_data(market_event) # This should fill the order in SimBroker

        open_orders = await gateway.get_open_orders("MNQ")
        logger.info(f"Gateway open orders for MNQ: {open_orders}")

        positions = await gateway.get_positions("MNQ")
        logger.info(f"Gateway positions for MNQ: {positions}")

        account_summary = await gateway.get_account_summary()
        logger.info(f"Gateway account summary: {account_summary}")

        await gateway.disconnect()

    asyncio.run(main_gateway_test())
