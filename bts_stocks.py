from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.data import BarType
from nautilus_trader.model.objects import Money
from nautilus_trader.model.currencies import USD
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.model.identifiers import Venue
import importlib.util
from pathlib import Path
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(os.getenv('DATA_DIR')) / 'stocks' / 'alpaca_2024_Q4_to_2025_Q3' / 'minute_interval'
LOG_DIR = os.getenv('LOG_DIR')

ticker_table = pd.read_feather('bt_tickers.feather')
tickers = ticker_table.index.to_list()

for ticker in tickers:
    current_time = pd.Timestamp.now()
    current_time_formatted = current_time.strftime('%Y-%m-%d-%H:%M:%S')
    my_log_file_name = f"naut_bt_{ticker}_{current_time_formatted}.log"
    engine_config = BacktestEngineConfig(
        logging=LoggingConfig(
            log_level="WARNING",
            log_level_file="INFO",
            log_file_name=my_log_file_name,
            log_directory=LOG_DIR
        )
    )
    engine = BacktestEngine(
        config=engine_config       
    )

    SIM_VENUE = Venue("SIM")

    INSTRUMENT = TestInstrumentProvider.equity(symbol=ticker, venue='SIM')

    BARTYPE = BarType.from_str(f'{ticker}.SIM-1-MINUTE-LAST-EXTERNAL')
    df = pd.read_feather(DATA_DIR / f'{ticker}.feather')
    wrangler = BarDataWrangler(BARTYPE, INSTRUMENT)
    bars_list = wrangler.process(df)

    engine.add_venue(
        venue=SIM_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(100_000, USD)],
        base_currency=USD
    )

    engine.add_instrument(INSTRUMENT)

    engine.add_data(bars_list)

    strategy_path = './sc.py'
    spec = importlib.util.spec_from_file_location("zscore", strategy_path)
    strategy_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy_module)

    strat = strategy_module.ZScoreMeanReversionStrategy
    ConfigClass = strategy_module.ZScoreMeanReversionConfig

    config_instance = ConfigClass(
        instrument_id=INSTRUMENT.id,
        bar_type=BARTYPE,
        z_lookback = 240,
        stop_loss_atr_multiple = 4.0,
        atr_period = 240 
    )

    instance = strat(config=config_instance)
    engine.add_strategy(strategy=instance)
    engine.run()

    symbol = str(INSTRUMENT.symbol)
    adf = ticker_table.loc[ticker, 'adf']
    natr = ticker_table.loc[ticker, 'natr_mean']
    stats_pnls = engine.portfolio.analyzer.get_performance_stats_pnls()
    stats_returns = engine.portfolio.analyzer.get_performance_stats_returns()
    pnl_pct = stats_pnls['PnL% (total)']
    sharpe = stats_returns['Sharpe Ratio (252 days)']
    account_report = engine.trader.generate_account_report(SIM_VENUE)
    ab_series = account_report['total'].astype(float)
    lowest_balance = ab_series.min()

    if ticker == symbol:
        print("Ticker", ticker)
    else:
        raise ValueError(f"Missmatch between {ticker} and {symbol}")

    print("Ticker ADF:", adf)
    print("Ticker Mean NATR:", natr)
    print("PnL%:", pnl_pct)
    print("Sharpe:", sharpe)
    print("Lowest Balance:", lowest_balance)
    print("--------------")

    engine.reset()