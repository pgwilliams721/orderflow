# Delta Rollover Bot

High-performance trading bot for detecting and trading the "Delta Rollover → Pullback" pattern on Bookmap & SpotGamma data, initially targeting MNQ/ES futures.

## Disclaimer

**Trading futures and options involves substantial risk of loss and is not suitable for all investors. Past performance is not necessarily indicative of future results.**

***This bot is for educational and research purposes only. Use at your own risk. The author and contributors are not responsible for any financial losses incurred by using this software.***

## Features (Current & Planned)

*   **Pattern Detection**: Core logic to identify the "Delta Rollover → Pullback" pattern.
    *   Aggressive Imbalance Stage (Implemented)
    *   Accumulation Stage (Partially Implemented in `PatternDetector`, needs completion)
    *   Rollover Confirmation & Breakout (To be Implemented)
*   **Data Ingestion**:
    *   **Bookmap**: Connector for live WebSocket data and historical `.bmq` file replay (current `.bmq` parsing is placeholder).
    *   **SpotGamma**: Connector for SPX/NDX Gamma levels, HIRO delta, and Gamma Flip via REST API with caching.
*   **Trading**:
    *   Pluggable trading gateway (`TradeGateway`).
    *   `SimBroker` for backtesting with simulated order execution.
    *   (Planned) `TradeovateBroker` for live trading.
*   **Risk Management**:
    *   ATR-based profit targets and stops (calculation logic in `RiskManager`).
    *   Trailing stops after TP1 hit.
    *   Daily loss limit and max position size checks.
*   **Backtesting Engine**: Allows replaying historical data, simulating trades, and evaluating basic performance (P&L, Max Drawdown).
*   **Configuration**: Settings managed via `config.yaml`.
*   **Logging**: JSON logs to file, pretty console logging, and CSV logging for fills (from `SimBroker`).
*   **Alerting**: Webhook alerts and placeholder for Telegram alerts.
*   **Jupyter Notebook Example**: Demonstrates backtesting setup and basic results visualization.
*   **Unit Tests**: Initial tests for `ConfigLoader` (aiming for >80% overall coverage).
*   **Documentation**: (Planned) MkDocs site.

## Project Structure

```
delta_rollover_bot/
├── config/                     # Configuration files (config.yaml, tradeovate_credentials.yaml.template)
├── core/                       # Core logic (pattern_detector.py)
├── data/                       # Data connectors (bookmap_connector.py, spotgamma_connector.py, data_provider_interface.py)
├── docs/                       # (Planned) Documentation files for MkDocs
├── tests/                      # Unit and integration tests
├── trading/                    # Trading gateway, broker implementations, risk management
│   ├── broker_interface.py
│   ├── sim_broker.py
│   ├── trade_gateway.py
│   ├── risk_manager.py
│   └── (planned) tradeovate_broker.py
├── utils/                      # Utility modules (logging_utils.py, alerting_utils.py)
├── __init__.py                 # Package initializer
├── backtester.py               # Backtesting engine
└── config_loader.py            # Configuration loading logic

logs/                           # Default directory for JSON log files
trades_history/                 # Default directory for CSV fill logs from SimBroker

example_backtest_and_analysis.ipynb # Jupyter notebook example
README.md                       # This file
requirements.txt                # Python package dependencies
setup.py                        # Package setup script
```

## Getting Started

### 1. Prerequisites

*   Python 3.8+
*   Git

### 2. Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd delta_rollover_bot
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\\Scripts\\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Install the package in editable mode:**
    ```bash
    pip install -e .
    ```

### 3. Configuration

1.  **Main Configuration (`config/config.yaml`):**
    *   Copy `delta_rollover_bot/config/config.yaml` (if it's a template in the repo) or review the existing `delta_rollover_bot/config/config.yaml`.
    *   **API Keys**:
        *   `api_keys.bookmap_api_key`: Your Bookmap API key (for live data).
        *   `api_keys.spotgamma_api_token`: Your SpotGamma API token.
    *   **Trading Parameters**: Adjust instrument, quantity, ATR multipliers, etc.
    *   **Pattern Thresholds**: Fine-tune parameters for pattern detection.
    *   **Data Paths**:
        *   `data_providers.bookmap.historical_data_path`: Set the path to your directory containing Bookmap `.bmq` historical data files (e.g., `test_data/` for the dummy file, or `path/to/your/real_bmq_files/`). Files should be named like `MNQ.bmq`, `ES.bmq`.
    *   **Logging & Alerting**:
        *   `logging.log_level`, `logging.log_file`, `logging.trades_csv_file`.
        *   `alerting.webhook_url` (e.g., for Slack/Discord).

2.  **Tradeovate Credentials (for live trading - when implemented):**
    *   The file `delta_rollover_bot/config/tradeovate_credentials.yaml` is used for Tradeovate API credentials.
    *   It's loaded by `ConfigLoader` and merged if present. It's recommended to **add this specific file to your `.gitignore`** if you populate it with real credentials.
    *   **Example `tradeovate_credentials.yaml` structure:**
        ```yaml
        tradeovate:
          tradeovate_user: "YOUR_TRADEOVATE_USERNAME"
          tradeovate_pass: "YOUR_TRADEOVATE_PASSWORD"
          tradeovate_app_id: "YOUR_TRADEOVATE_APP_ID_OR_KEY"
          tradeovate_cid: 12345 # Your Customer ID (integer)
          tradeovate_env: "demo" # "demo" or "live"
        ```

## Usage

### 1. Running a Backtest

The primary way to run a backtest is using the example Jupyter Notebook:

*   **`example_backtest_and_analysis.ipynb`**:
    *   Open this notebook in Jupyter Lab or Jupyter Notebook.
    *   It provides a step-by-step guide to:
        *   Load configuration (uses a dummy config by default, can be adapted).
        *   Initialize and run the `Backtester`.
        *   (Crucially) It shows how to inject a test pattern signal, as the `PatternDetector` might not yet be complete enough to find signals from the limited simulated data that `BookmapConnector`'s historical mode provides by default if a real `.bmq` file is not properly parsed.
        *   Display basic performance metrics (P&L, Max Drawdown) and plot the equity curve.
    *   **To use your own `.bmq` data**:
        1.  Ensure your `.bmq` file (e.g., `MNQ.bmq`) is in the directory specified by `data_providers.bookmap.historical_data_path` in your `config.yaml` (or the notebook's dummy config).
        2.  **Important**: The current `BookmapConnector` has placeholder logic for `.bmq` processing. True backtesting on `.bmq` files requires implementing a proper parser for this format. The backtester currently relies on simulated ticks from `BookmapConnector` if the file is present but not deeply parsed.

### 2. (Planned) Live Trading

*   (Details to be added once `TradeovateBroker` and main application logic for live mode are implemented.)
*   Typically, this would involve running a main script:
    ```bash
    # python -m delta_rollover_bot.main --mode live --config path/to/your/config.yaml
    ```

### 3. Running Unit Tests

```bash
python -m pytest delta_rollover_bot/tests/ --cov=delta_rollover_bot
```

### 4. Building Documentation (Planned)

```bash
mkdocs build
# mkdocs serve (for local preview)
```

## Development Notes

*   **Pattern Logic**: The core pattern detection in `PatternDetector` is under active development.
*   **`.bmq` Parsing**: Proper parsing of Bookmap HD files (`.bmq`) is a complex task due to the proprietary format and is currently a placeholder in `BookmapConnector`. For accurate backtesting, this needs to be addressed.
*   **Indicators**: ATR, RSI, VWAP calculations are planned and will be integrated into `PatternDetector` and `RiskManager`.

## Contributing

(Details to be added if contributions are open.)

---
*This bot is for educational and research purposes only. Use at your own risk.*