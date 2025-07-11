from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable, Awaitable
from enum import Enum, auto

# Define common types for orders, positions, etc.

class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()
    STOP_MARKET = auto()
    STOP_LIMIT = auto()
    # Add more if needed, e.g., TRAILING_STOP

class OrderSide(Enum):
    BUY = auto()
    SELL = auto()

class OrderStatus(Enum):
    PENDING_NEW = auto() # Not yet acknowledged by broker
    NEW = auto()         # Accepted by broker, not yet filled
    PARTIALLY_FILLED = auto()
    FILLED = auto()
    CANCELLED = auto()
    PENDING_CANCEL = auto()
    REJECTED = auto()
    EXPIRED = auto()
    ERROR = auto() # Local error before sending or other issues

class Order:
    def __init__(self,
                 instrument: str,
                 side: OrderSide,
                 order_type: OrderType,
                 quantity: float,
                 limit_price: Optional[float] = None,
                 stop_price: Optional[float] = None,
                 order_id: Optional[str] = None, # Broker-assigned ID
                 client_order_id: Optional[str] = None, # User-assigned ID
                 status: OrderStatus = OrderStatus.PENDING_NEW,
                 avg_fill_price: Optional[float] = None,
                 filled_quantity: float = 0.0,
                 timestamp_ns: Optional[int] = None, # Nanosecond timestamp for creation/update
                 message: Optional[str] = None): # For rejections or other info
        self.instrument = instrument
        self.side = side
        self.order_type = order_type
        self.quantity = quantity
        self.limit_price = limit_price
        self.stop_price = stop_price
        self.order_id = order_id
        self.client_order_id = client_order_id if client_order_id else f"co_{int(time.time()*1e9)}" # Default client id
        self.status = status
        self.avg_fill_price = avg_fill_price
        self.filled_quantity = filled_quantity
        self.timestamp_ns = timestamp_ns if timestamp_ns else int(time.time() * 1e9)
        self.message = message

    def __repr__(self):
        return (f"Order(id={self.order_id}, client_id={self.client_order_id}, inst={self.instrument}, "
                f"{self.side.name} {self.quantity} @ {self.order_type.name} "
                f"Lmt:{self.limit_price} Stp:{self.stop_price}, Status:{self.status.name}, "
                f"FilledQty:{self.filled_quantity}, AvgPx:{self.avg_fill_price})")

class Position:
    def __init__(self,
                 instrument: str,
                 quantity: float, # Positive for long, negative for short
                 average_entry_price: float,
                 unrealized_pnl: Optional[float] = None,
                 realized_pnl: Optional[float] = None):
        self.instrument = instrument
        self.quantity = quantity
        self.average_entry_price = average_entry_price
        self.unrealized_pnl = unrealized_pnl
        self.realized_pnl = realized_pnl

    def __repr__(self):
        return (f"Position(inst={self.instrument}, qty={self.quantity}, avg_px={self.average_entry_price}, "
                f"UnrealPnL:{self.unrealized_pnl}, RealPnL:{self.realized_pnl})")

class Fill:
    def __init__(self,
                 order_id: str,
                 client_order_id: str,
                 fill_id: str,
                 instrument: str,
                 side: OrderSide,
                 price: float,
                 quantity: float,
                 timestamp_ns: int,
                 commission: Optional[float] = None,
                 fee: Optional[float] = None):
        self.order_id = order_id
        self.client_order_id = client_order_id
        self.fill_id = fill_id
        self.instrument = instrument
        self.side = side
        self.price = price
        self.quantity = quantity
        self.timestamp_ns = timestamp_ns
        self.commission = commission
        self.fee = fee

    def __repr__(self):
        return (f"Fill(id={self.fill_id}, ord_id={self.order_id}, client_ord_id={self.client_order_id}, "
                f"inst={self.instrument}, {self.side.name} {self.quantity} @ {self.price}, ts={self.timestamp_ns})")


# Define callback types for broker events
OrderUpdateCallback = Callable[[Order], Awaitable[None]]
FillUpdateCallback = Callable[[Fill], Awaitable[None]]
PositionUpdateCallback = Callable[[Position], Awaitable[None]]
AccountSummaryCallback = Callable[[Dict[str, Any]], Awaitable[None]] # e.g. balance, margin


class BrokerInterface(ABC):
    """
    Abstract base class for all broker implementations.
    """
    def __init__(self, config: Any, event_loop: asyncio.AbstractEventLoop):
        self.config = config
        self.loop = event_loop
        self._order_update_callbacks: List[OrderUpdateCallback] = []
        self._fill_update_callbacks: List[FillUpdateCallback] = []
        self._position_update_callbacks: List[PositionUpdateCallback] = []
        self._account_summary_callbacks: List[AccountSummaryCallback] = []

    @abstractmethod
    async def connect(self):
        """Connect to the broker's API."""
        pass

    @abstractmethod
    async def disconnect(self):
        """Disconnect from the broker's API."""
        pass

    @abstractmethod
    async def place_order(self, order: Order) -> Order:
        """
        Place an order.
        Returns the order object, potentially updated with an order_id from the broker
        and status PENDING_NEW or NEW if accepted, or REJECTED/ERROR.
        """
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str, client_order_id: Optional[str] = None) -> Order:
        """
        Cancel an existing order.
        Returns the order object with status PENDING_CANCEL or CANCELLED if successful.
        """
        pass

    @abstractmethod
    async def modify_order(self, order_id: str, new_quantity: Optional[float] = None,
                           new_limit_price: Optional[float] = None,
                           new_stop_price: Optional[float] = None,
                           client_order_id: Optional[str] = None) -> Order:
        """
        Modify an existing order (e.g., price or quantity).
        Not all brokers support all types of modifications.
        Returns the modified order.
        """
        pass

    @abstractmethod
    async def get_order_status(self, order_id: str, client_order_id: Optional[str] = None) -> Optional[Order]:
        """Fetch the status of a specific order."""
        pass

    @abstractmethod
    async def get_open_orders(self, instrument: Optional[str] = None) -> List[Order]:
        """Fetch all open orders, optionally filtered by instrument."""
        pass

    @abstractmethod
    async def get_positions(self, instrument: Optional[str] = None) -> List[Position]:
        """Fetch current positions, optionally filtered by instrument."""
        pass

    @abstractmethod
    async def get_account_summary(self) -> Dict[str, Any]:
        """Fetch account summary (balance, margin, P&L, etc.)."""
        pass

    # --- Callback registration methods ---
    def register_order_update_callback(self, callback: OrderUpdateCallback):
        if callback not in self._order_update_callbacks:
            self._order_update_callbacks.append(callback)

    def register_fill_update_callback(self, callback: FillUpdateCallback):
        if callback not in self._fill_update_callbacks:
            self._fill_update_callbacks.append(callback)

    def register_position_update_callback(self, callback: PositionUpdateCallback):
        if callback not in self._position_update_callbacks:
            self._position_update_callbacks.append(callback)

    def register_account_summary_callback(self, callback: AccountSummaryCallback):
        if callback not in self._account_summary_callbacks:
            self._account_summary_callbacks.append(callback)

    # --- Methods to be called by implementations to publish updates ---
    async def _publish_order_update(self, order: Order):
        for cb in self._order_update_callbacks:
            asyncio.create_task(cb(order)) # Fire and forget

    async def _publish_fill_update(self, fill: Fill):
        # This might also trigger an order update and position update
        for cb in self._fill_update_callbacks:
            asyncio.create_task(cb(fill))

    async def _publish_position_update(self, position: Position):
        for cb in self._position_update_callbacks:
            asyncio.create_task(cb(position))

    async def _publish_account_summary_update(self, summary: Dict[str, Any]):
        for cb in self._account_summary_callbacks:
            asyncio.create_task(cb(summary))

# Need to import time for default client_order_id
import time
import asyncio # For event_loop type hint

if __name__ == '__main__': # pragma: no cover
    # Example of how the classes might be used (for illustration)

    # Create an order
    buy_order = Order(
        instrument="MNQZ3",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=1,
        limit_price=15000.00
    )
    print(buy_order)

    # Create a position
    current_pos = Position(
        instrument="MNQZ3",
        quantity=2,
        average_entry_price=14950.00,
        unrealized_pnl=100.00
    )
    print(current_pos)

    # Create a fill
    fill_event = Fill(
        order_id="broker123",
        client_order_id=buy_order.client_order_id,
        fill_id="fill_xyz",
        instrument="MNQZ3",
        side=OrderSide.BUY,
        price=15000.00,
        quantity=1,
        timestamp_ns=int(time.time() * 1e9)
    )
    print(fill_event)

    # Dummy callback functions
    async def my_order_handler(order: Order):
        print(f"Callback: Order Update Received: {order}")

    async def my_fill_handler(fill: Fill):
        print(f"Callback: Fill Update Received: {fill}")

    # In a real scenario, a concrete broker implementation would call these:
    # await broker_instance._publish_order_update(buy_order_after_broker_ack)
    # await broker_instance._publish_fill_update(fill_event)
