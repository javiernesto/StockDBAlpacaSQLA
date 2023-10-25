import json
import os
import sys
from datetime import datetime, timedelta
from alpaca_trade_api import REST, TimeFrame

main_path = os.path.dirname(os.path.realpath(__file__))
config_file = os.path.join(main_path, 'app_config.json')
dir_path = os.path.join(main_path, 'data_model\\')
sys.path.insert(0, dir_path)

import data_model.stock_model as sm


if __name__ == '__main__':
    # Loading the json file
    with open(config_file, "r") as f:
        conf = json.load(f)

        # Loading the database model with the database configuration in the json file
        db_stock = sm.StockModel(conf['database'].format(main_path))

        #Getting the API key, secrect key and base URL for the alpaca trade API
        api_key = conf['API_KEY']
        secret_key = conf['SECRET_KEY']
        base_url = conf['BASE_URL']

        # Setting the date range of stock prices we want to get
        end_date = datetime.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=365 * 2)

        # Inserting the 3M company informaation to the stock table
        db_stock.add_update_stock('MMM', '3M Company', 'Industrials', True)

        # Setting the symbols we want to get prices for
        symbols = ['MMM']

        # Instantiating the alpaca API
        api = REST(key_id=api_key, secret_key=secret_key,
                base_url=base_url, api_version='v2')
        print('Fetching bars from {} to {}'.format(start_date, end_date))

        # Getting the adjusted daily stock prices for MMM
        df_barset = api.get_bars(symbols, TimeFrame.Day, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'),
                                adjustment='all').df
        df_barset = df_barset.reset_index(level=0)

        # Saving the stock prices in the database
        db_stock.add_update_stock_prices(df_barset)
        print('Done fetching bars')