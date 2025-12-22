from statsmodels.tsa.stattools import adfuller
import pandas_ta as ta
from multiprocessing import Pool, cpu_count
import logging
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
import time
from tqdm import tqdm

start_time = time.perf_counter()

env_path = '../.env'
load_dotenv(env_path)

SYS_DATA_ROOT = Path(os.getenv('DATA_DIR'))
PROJECT_DATA_ROOT = SYS_DATA_ROOT / 'stocks' / 'alpaca_2024_Q4_to_2025_Q3'
min_data_dir = PROJECT_DATA_ROOT / 'minute_interval'
min_data_dir.mkdir(parents=True, exist_ok=True)
screen_data_temp = SYS_DATA_ROOT / 'screen_results_temp'
screen_data_temp.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename='screener.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)
logger = logging.getLogger(__name__)

def calc_adf(close):
    adf = adfuller(close, maxlag=20, autolag=None)
    adf, p, crit_10 = adf[0], adf[1], adf[4]['10%']
    below_10 = (adf < crit_10)
    return adf, crit_10, below_10, p

def calc_stats(index, df: pd.DataFrame):
    row = pd.DataFrame(index=[index]) 
    row['total_volume'] = df['volume'].sum()
    row['adf'], row['adf_10%_level'], row['below_10%'], row['adf_p'] = calc_adf(df['close'])
    natr_series = ta.natr(df['high'], df['low'], df['close'], length=120).dropna()
    min_natr, mean_natr = natr_series.min(), natr_series.mean()
    row['natr_min'], row['natr_mean'] = min_natr, mean_natr
    min_price, mean_price, max_price = df['close'].min(), df['close'].mean(), df['close'].max()
    row['min_price'], row['mean_price'], row['max_price'] = min_price, mean_price, max_price
    return row

results = pd.DataFrame()
feather_files = list(min_data_dir.glob('*.feather'))

for fp in tqdm(feather_files):
    ticker = fp.stem
    temp_path = screen_data_temp / f'{ticker}.feather' 
    if temp_path.exists():
        logger.info(f'Results for {ticker} already exist')
        continue

    df = pd.read_feather(fp)
    stats_row = calc_stats(ticker, df)
    stats_row.to_feather(temp_path)
    logger.info(f"Processed {ticker}")

for fp in screen_data_temp.glob('*.feather'):
    df = pd.read_feather(fp)
    results = pd.concat([results, df])

results.index.name = 'ticker'
current_time = pd.Timestamp.now()
current_time_formatted = current_time.strftime('%Y-%m-%d-%H:%M')
results.to_csv(f'./results_1min_{current_time_formatted}.csv')

end_time = time.perf_counter()
elapsed_time = end_time - start_time
print(f"Completed in {elapsed_time}.")