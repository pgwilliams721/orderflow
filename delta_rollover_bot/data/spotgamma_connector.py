import asyncio
import aiohttp
import logging
import time
from typing import Dict, Any, Optional

from delta_rollover_bot.data.data_provider_interface import DataProviderInterface, DataEvent
from delta_rollover_bot.config_loader import Config

logger = logging.getLogger(__name__)

class SpotGammaConnector(DataProviderInterface):
    """
    Connects to SpotGamma REST API to fetch gamma levels, HIRO delta, and Gamma Flip.
    Implements intraday caching for fetched data.
    """
    # Define SpotGamma API endpoints (these are examples, replace with actual endpoints)
    # These would typically be for specific indices like SPX or NDX.
    # The API might require the index to be part of the URL path or as a query parameter.
    # Example: "https://api.spotgamma.com/v1/gamma_levels?index=SPX"
    SPOTGAMMA_API_BASE_URL = "https://api.spotgamma.com/v1" # Placeholder
    GAMMA_LEVELS_ENDPOINT = "/gamma/levels" # Example: SPX/NDX Gamma levels
    HIRO_DELTA_ENDPOINT = "/hiro" # Example: HIRO delta
    GAMMA_FLIP_ENDPOINT = "/gamma/flip" # Example: Daily "Gamma Flip"

    def __init__(self, event_queue: asyncio.Queue, config: Config, index_symbol: str = "SPX"):
        super().__init__(event_queue, config)
        self.api_token = self.config.get("api_keys.spotgamma_api_token")
        self.index_symbol = index_symbol # e.g., "SPX", "NDX"
        self.cache_duration = self.config.get("data_providers.spotgamma.cache_duration_seconds", 300)

        self._session: Optional[aiohttp.ClientSession] = None
        self._cached_data: Dict[str, Any] = {} # Cache for different data types
        self._cache_timestamps: Dict[str, float] = {} # Timestamps for cache entries

        self._task: Optional[asyncio.Task] = None

        if not self.api_token or self.api_token == "YOUR_SPOTGAMMA_TOKEN":
            logger.warning("SpotGamma API token is not configured. Data fetching will be disabled.")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Initializes and returns an aiohttp.ClientSession."""
        if self._session is None or self._session.closed:
            headers = {
                "Authorization": f"Bearer {self.api_token}"
                # Add other common headers if required by SpotGamma API
            }
            self._session = aiohttp.ClientSession(headers=headers)
        return self._session

    async def connect(self):
        """
        Initialize the HTTP session. For a REST API, 'connect' mostly means
        being ready to make requests.
        """
        if not self.api_token or self.api_token == "YOUR_SPOTGAMMA_TOKEN":
            logger.error("SpotGammaConnector: Cannot connect, API token missing.")
            return

        await self._get_session() # Initializes the session
        logger.info(f"SpotGammaConnector initialized for {self.index_symbol}. Ready to fetch data.")

    async def disconnect(self):
        """
        Close the HTTP session.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("SpotGammaConnector: HTTP session closed.")
        self._session = None
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("SpotGamma data fetching task cancelled.")
        logger.info(f"SpotGammaConnector disconnected for {self.index_symbol}.")


    async def _fetch_data(self, endpoint_suffix: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Generic method to fetch data from a SpotGamma API endpoint.
        """
        if not self.api_token or self.api_token == "YOUR_SPOTGAMMA_TOKEN":
            logger.warning(f"SpotGamma API token missing. Cannot fetch {endpoint_suffix}.")
            return None
        if not self._is_running: # Do not fetch if not running
            return None

        session = await self._get_session()
        url = f"{self.SPOTGAMMA_API_BASE_URL}{endpoint_suffix}"

        # Add index symbol to params if not already there, common requirement
        request_params = params.copy() if params else {}
        if 'index' not in request_params and 'symbol' not in request_params:
            request_params['index'] = self.index_symbol

        try:
            logger.debug(f"SpotGamma: Fetching {url} with params {request_params}")
            async with session.get(url, params=request_params) as response:
                response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
                data = await response.json()
                logger.debug(f"SpotGamma: Successfully fetched data from {url}. Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
                return data
        except aiohttp.ClientResponseError as e:
            logger.error(f"SpotGamma API request to {url} failed with status {e.status}: {e.message}")
        except aiohttp.ClientConnectionError as e:
            logger.error(f"SpotGamma API connection error for {url}: {e}")
        except json.JSONDecodeError:
            logger.error(f"SpotGamma: Failed to decode JSON response from {url}")
        except Exception as e: # pylint: disable=broad-except
            logger.error(f"SpotGamma: Unexpected error fetching data from {url}: {e}", exc_info=True)
        return None

    def _get_cached_data(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves data from cache if valid.
        """
        if cache_key in self._cached_data and \
           (time.time() - self._cache_timestamps.get(cache_key, 0)) < self.cache_duration:
            logger.debug(f"SpotGamma: Using cached data for {cache_key}")
            return self._cached_data[cache_key]
        return None

    def _update_cache(self, cache_key: str, data: Dict[str, Any]):
        """
        Updates the cache with new data.
        """
        self._cached_data[cache_key] = data
        self._cache_timestamps[cache_key] = time.time()
        logger.debug(f"SpotGamma: Cache updated for {cache_key}")

    async def fetch_gamma_levels(self, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        cache_key = f"gamma_levels_{self.index_symbol}"
        if not force_refresh:
            cached = self._get_cached_data(cache_key)
            if cached: return cached

        data = await self._fetch_data(self.GAMMA_LEVELS_ENDPOINT)
        if data:
            self._update_cache(cache_key, data)
            # Example: Publish an event with the new gamma levels
            # The structure of 'data' depends on SpotGamma's actual API response
            event_data = DataEvent(
                event_type=f"spotgamma_gamma_levels_{self.index_symbol.lower()}",
                data={"levels": data, "index": self.index_symbol} # Adjust payload as needed
            )
            await self._publish_event(event_data)
        return data

    async def fetch_hiro_delta(self, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        cache_key = f"hiro_delta_{self.index_symbol}"
        if not force_refresh:
            cached = self._get_cached_data(cache_key)
            if cached: return cached

        data = await self._fetch_data(self.HIRO_DELTA_ENDPOINT)
        if data:
            self._update_cache(cache_key, data)
            event_data = DataEvent(
                event_type=f"spotgamma_hiro_delta_{self.index_symbol.lower()}",
                data={"hiro": data, "index": self.index_symbol} # Adjust payload
            )
            await self._publish_event(event_data)
        return data

    async def fetch_gamma_flip(self, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        cache_key = f"gamma_flip_{self.index_symbol}" # Gamma flip is usually daily, cache might be longer
        if not force_refresh:
            cached = self._get_cached_data(cache_key)
            if cached: return cached

        data = await self._fetch_data(self.GAMMA_FLIP_ENDPOINT)
        if data:
            self._update_cache(cache_key, data)
            event_data = DataEvent(
                event_type=f"spotgamma_gamma_flip_{self.index_symbol.lower()}",
                data={"flip_data": data, "index": self.index_symbol} # Adjust payload
            )
            await self._publish_event(event_data)
        return data

    async def _periodic_fetch_loop(self):
        """
        Periodically fetches all relevant SpotGamma data according to their typical update frequencies
        or a general configured interval.
        """
        await self.connect() # Ensure session is ready

        while self._is_running:
            try:
                logger.info(f"SpotGamma ({self.index_symbol}): Performing periodic data fetch.")
                # Fetch all relevant data points. Force refresh can be false to respect cache,
                # but the loop itself ensures periodic checks.
                # If an endpoint updates less frequently (e.g., Gamma Flip daily),
                # the cache will prevent redundant API calls.
                await self.fetch_gamma_levels(force_refresh=True) # Or based on specific needs
                await asyncio.sleep(1) # Small delay between requests if hitting same server
                await self.fetch_hiro_delta(force_refresh=True)
                await asyncio.sleep(1)
                await self.fetch_gamma_flip(force_refresh=True) # Often daily, cache will handle it

                # Wait for the cache duration or a configured refresh interval before next fetch cycle
                # Using cache_duration as the loop interval ensures data is attempted to be refreshed
                # when it's considered stale.
                logger.debug(f"SpotGamma ({self.index_symbol}): Periodic fetch complete. Waiting for {self.cache_duration}s.")
                await asyncio.sleep(self.cache_duration)

            except asyncio.CancelledError:
                logger.info(f"SpotGamma ({self.index_symbol}): Periodic fetch loop cancelled.")
                break
            except Exception as e: # pylint: disable=broad-except
                logger.error(f"SpotGamma ({self.index_symbol}): Error in periodic fetch loop: {e}", exc_info=True)
                # Wait a bit before retrying on generic error to avoid spamming
                await asyncio.sleep(60)

        logger.info(f"SpotGamma ({self.index_symbol}): Periodic fetch loop stopped.")


    async def start_streaming(self):
        """
        Starts the periodic fetching of SpotGamma data.
        SpotGamma is typically not a "streaming" source in the WebSocket sense,
        but we can simulate a stream by periodically polling the API.
        """
        if not self.api_token or self.api_token == "YOUR_SPOTGAMMA_TOKEN":
            logger.error("SpotGammaConnector: Cannot start streaming, API token missing.")
            return

        await super().start_streaming() # Sets self._is_running = True
        logger.info(f"SpotGammaConnector: Starting periodic data fetching for {self.index_symbol}.")

        if self._task and not self._task.done():
            logger.warning(f"SpotGamma ({self.index_symbol}): Fetch task already running.")
        else:
            self._task = asyncio.create_task(self._periodic_fetch_loop())

    async def stop_streaming(self):
        """
        Stops the periodic fetching task.
        """
        await super().stop_streaming() # Sets self._is_running = False
        logger.info(f"SpotGammaConnector: Stopping periodic data fetching for {self.index_symbol}.")
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info(f"SpotGamma ({self.index_symbol}): Fetch task successfully cancelled during stop.")
        self._task = None
        # Disconnect will close the session, typically called by the managing component
        # await self.disconnect() # Or let the manager handle disconnect


if __name__ == '__main__': # pragma: no cover
    # Basic test and example usage
    async def main_test():
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Create a dummy config for testing
        class SimpleTestConfig:
            def __init__(self, data):
                self._data = data
            def get(self, key, default=None):
                keys = key.split('.')
                val = self._data
                try:
                    for k in keys: val = val[k]
                    return val
                except KeyError: return default
                except TypeError: return default # Handle if val is not a dict

        # IMPORTANT: To test this properly, you would need:
        # 1. A valid SpotGamma API token.
        # 2. Correct SpotGamma API base URL and endpoint paths.
        # 3. The SpotGamma API to be live and accessible.
        # The following test will likely log errors if these are not met.
        # Replace "YOUR_SPOTGAMMA_TOKEN_FOR_TESTING" with a real token if you have one.
        # Ensure the base URL and endpoints in the class are correct.

        sg_api_token = "YOUR_SPOTGAMMA_TOKEN_FOR_TESTING"
        if sg_api_token == "YOUR_SPOTGAMMA_TOKEN_FOR_TESTING" or sg_api_token == "YOUR_SPOTGAMMA_TOKEN":
            logger.warning("Using placeholder SpotGamma token. API calls will likely fail or be skipped.")
            # To prevent actual API calls with dummy token, we can skip parts of the test
            # or expect failures. For now, it will try and log errors.

        test_config_sg = SimpleTestConfig({
            "api_keys": {"spotgamma_api_token": sg_api_token},
            "data_providers": {
                "spotgamma": {
                    "cache_duration_seconds": 10 # Short cache for testing
                }
            }
        })

        event_q_sg = asyncio.Queue()
        # Test for SPX
        sg_connector_spx = SpotGammaConnector(event_q_sg, config=test_config_sg, index_symbol="SPX")
        # Test for NDX (uses the same queue for simplicity in this test)
        sg_connector_ndx = SpotGammaConnector(event_q_sg, config=test_config_sg, index_symbol="NDX")

        async def sg_consumer():
            logger.info("SpotGamma Consumer started. Waiting for events...")
            try:
                while True:
                    event = await asyncio.wait_for(event_q_sg.get(), timeout=30) # Wait up to 30s for an event
                    logger.info(f"SPOTGAMMA_CONSUMER received: {event.event_type} for index {event.data.get('index')}")
                    # logger.debug(f"Full event data: {event.data}")
                    event_q_sg.task_done()
            except asyncio.TimeoutError:
                logger.info("SpotGamma Consumer timed out waiting for events. (Expected if API calls fail or no data).")
            except asyncio.CancelledError:
                logger.info("SpotGamma Consumer cancelled.")

        consumer_task_sg = asyncio.create_task(sg_consumer())

        logger.info("--- Starting SpotGamma Connectors (SPX & NDX) ---")
        await sg_connector_spx.connect()
        await sg_connector_ndx.connect()

        await sg_connector_spx.start_streaming() # Starts periodic fetching for SPX
        await sg_connector_ndx.start_streaming() # Starts periodic fetching for NDX

        # Let it run for a bit to see if it fetches (or tries to)
        # With a 10s cache, it should try to fetch twice if token was valid
        logger.info("SpotGamma connectors running. Waiting for 25 seconds to observe behavior...")
        await asyncio.sleep(25)

        logger.info("--- Stopping SpotGamma Connectors ---")
        await sg_connector_spx.stop_streaming()
        await sg_connector_ndx.stop_streaming()

        await sg_connector_spx.disconnect()
        await sg_connector_ndx.disconnect()

        # Allow consumer to finish processing any queued items and then cancel it
        try:
            await asyncio.wait_for(event_q_sg.join(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("SpotGamma consumer queue join timed out.")

        consumer_task_sg.cancel()
        try:
            await consumer_task_sg
        except asyncio.CancelledError:
            pass

        logger.info("--- SpotGammaConnector test finished ---")

    # Note: The test above will likely show errors if the API token/endpoints are not real,
    # as it attempts to make HTTP requests. The error handling in the connector should
    # manage these failures gracefully (e.g., log errors, return None).
    asyncio.run(main_test())
