from abc import ABC, abstractmethod
import asyncio
from typing import Callable, Any, Dict, Optional

class DataEvent:
    """
    Represents a generic data event from a provider.
    """
    def __init__(self, event_type: str, data: Dict[str, Any], timestamp: Optional[float] = None):
        self.event_type = event_type # e.g., "bookmap_tick", "spotgamma_update"
        self.data = data
        self.timestamp = timestamp if timestamp is not None else asyncio.get_event_loop().time()

    def __repr__(self):
        return f"DataEvent(type={self.event_type}, ts={self.timestamp}, data={self.data})"

# Define a callback type for data events
DataEventCallback = Callable[[DataEvent], asyncio.Future]

class DataProviderInterface(ABC):
    """
    Abstract base class for all data providers.
    """
    def __init__(self, event_queue: asyncio.Queue, config):
        self.event_queue = event_queue
        self.config = config
        self._is_running = False
        self._subscribers: list[DataEventCallback] = [] # For direct callback if needed, alternative to queue

    @abstractmethod
    async def connect(self):
        """
        Connect to the data source.
        For file-based sources, this might mean opening the file.
        """
        pass

    @abstractmethod
    async def disconnect(self):
        """
        Disconnect from the data source.
        For file-based sources, this might mean closing the file.
        """
        pass

    @abstractmethod
    async def start_streaming(self):
        """
        Start streaming data into the event_queue or to subscribers.
        This method should contain the main loop for fetching/processing data.
        """
        self._is_running = True
        pass

    async def stop_streaming(self):
        """
        Stop streaming data.
        """
        self._is_running = False

    async def _publish_event(self, event_data: DataEvent):
        """
        Puts an event into the shared asyncio.Queue.
        Also calls direct subscribers if any.
        """
        await self.event_queue.put(event_data)
        if self._subscribers:
            for callback in self._subscribers:
                asyncio.create_task(callback(event_data)) # Callbacks should be async

    def subscribe(self, callback: DataEventCallback):
        """
        Allows other components to subscribe to data events directly.
        Note: Events are also always published to the main event_queue.
        """
        if callback not in self._subscribers:
            self._subscribers.append(callback)

    def unsubscribe(self, callback: DataEventCallback):
        """
        Remove a direct subscriber.
        """
        try:
            self._subscribers.remove(callback)
        except ValueError:
            pass # Callback not found

    @property
    def is_running(self) -> bool:
        return self._is_running

if __name__ == '__main__': # pragma: no cover
    # Example of how DataEvent could be used (for illustration)
    async def main():
        event = DataEvent(event_type="sample_event", data={"key": "value", "num": 123})
        print(event)

        # Example of using the queue
        q = asyncio.Queue()

        class DummyProvider(DataProviderInterface):
            async def connect(self): print("Dummy connected")
            async def disconnect(self): print("Dummy disconnected")
            async def start_streaming(self):
                await super().start_streaming()
                print("Dummy streaming started")
                for i in range(3):
                    if not self.is_running: break
                    await asyncio.sleep(0.1)
                    ev = DataEvent("dummy_tick", {"count": i}, timestamp=asyncio.get_event_loop().time())
                    await self._publish_event(ev)
                print("Dummy streaming finished cycle")

        # Dummy config for the provider
        class DummyConfig:
            def get(self, key, default=None): return None

        provider = DummyProvider(q, DummyConfig())

        async def consumer():
            while True:
                item = await q.get()
                if item is None: # Sentinel for stopping
                    q.task_done()
                    break
                print(f"Consumed: {item}")
                q.task_done()

        consumer_task = asyncio.create_task(consumer())

        await provider.connect()
        await provider.start_streaming()
        # In a real scenario, start_streaming would run until stopped
        # For this example, it finishes its loop.
        # To stop it mid-stream:
        # asyncio.create_task(provider.stop_streaming())

        await q.join() # Wait for all items to be processed from queue
        await provider.disconnect()
        await q.put(None) # Signal consumer to stop
        await consumer_task


    asyncio.run(main())
