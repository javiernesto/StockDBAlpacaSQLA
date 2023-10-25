import pandas as pd
import base
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from stock import Stock
from price import Price

class StockModel(object):
    _db_config = ''
    engine = object

    def __init__(self, db_config):
        self._db_config = db_config
        self.engine = create_engine(self._db_config)
        with self.engine.begin() as conn:
            base.Base.metadata.create_all(conn)

    def get_query_result(self, query):
        with self.engine.begin() as conn:
            return conn.execute(query).fetchall()

    """
    Stock Table
    """

    def get_all_stocks(self):
        data = self.get_query_result(select(Stock))
        df_stock = pd.DataFrame(data, columns=['symbol', 'name', 'sector', 'sp_500'])
        return df_stock

    def add_update_stock(self, symbol, name, sector, sp_500):
        Session = sessionmaker(bind=self.engine, future=True)
        session = Session()
        try:
            row_stock = session.execute(
                select(
                    Stock
                ).filter_by(
                    symbol=symbol
                )
            ).scalar_one()
            if row_stock is not None:
                row_stock.name = name
                row_stock.sector = sector
                row_stock.sp_500 = sp_500
            else:
                new_stock = Stock(symbol, name, sector, sp_500)
                session.add(new_stock)
        except:
            new_stock = Stock(symbol, name, sector, sp_500)
            session.add(new_stock)
        session.commit()

    """
    Price Table
    """

    def add_update_stock_prices(self, stock_prices):
        Session = sessionmaker(bind=self.engine, future=True)
        session = Session()
        lst_stock_prices = [(Price(row.timestamp, row.symbol, row.open, row.close, row.high, row.low, row.volume,
        row.trade_count, row.vwap)) for index, row in stock_prices.iterrows() ]
        session.bulk_save_objects(lst_stock_prices)
        session.commit()

    def add_update_stock_price(self, day, symbol, open_price, close, high, low, volume, trade_count, vwap):
        Session = sessionmaker(bind=self.engine, future=True)
        session = Session()
        try:
            row_price = session.execute(
                select(
                    Price
                ).filter_by(
                    symbol=symbol,
                    day=day
                )
            ).scalar_one()
            if row_price is not None:
                row_price.open = open_price
                row_price.close = close
                row_price.high = high
                row_price.low = low
                row_price.volume = volume
                row_price.trade_count = trade_count
                row_price.vwap = vwap
            else:
                new_price = Price(day, symbol, open_price, close, high, low, volume, trade_count, vwap)
                session.add(new_price)
        except:
            new_price = Price(day, symbol, open_price, close, high, low, volume, trade_count, vwap)
            session.add(new_price)
        session.commit()