import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, Date, func, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.inspection import inspect
import pandas as pd
import numpy as np

from config import config


if config.postgres.host:
    engine = create_engine(
        f"postgresql://{config.postgres.user}:{config.postgres.password}@{config.postgres.host}:{config.postgres.port}/{config.postgres.database}"
    )
else:
    engine = create_engine(f"sqlite:///db.sqlite")

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"
    cik = Column(Integer, primary_key=True)
    name = Column(String(), unique=True, nullable=False)
    ticker = Column(String())

    @staticmethod
    def create(cik, ticker, name):
        with Session() as session:
            c = Company()
            c.cik = cik
            c.ticker = ticker
            c.name = name
            session.add(c)
            session.commit()
        return c

    @staticmethod
    def get(ticker):
        try:

            with Session() as session:
                c = session.query(Company).filter(Company.ticker == ticker).one()
            return c
        except sqlalchemy.exc.NoResultFound:
            return None

    @staticmethod
    def list():
        with Session() as session:
            result = session.query(Company).order_by(Company.ticker).all()
        return result

    @staticmethod
    def upsert(items):
        df = pd.DataFrame(items)
        with engine.begin() as connection:
            df.to_sql("companies", index=False, con=connection, if_exists="replace")


class Price(Base):
    __tablename__ = "prices"
    date = Column(Date, primary_key=True, nullable=False)
    symbol = Column(String(), primary_key=True, nullable=False)
    adj_close = Column(Float())
    open = Column(Float())
    close = Column(Float())
    high = Column(Float())
    low = Column(Float())
    volume = Column(Float())

    @staticmethod
    def get(tickers, start, end):

        with Session() as session:
            results = pd.read_sql(
                session.query(Price)
                .filter(Price.symbol.in_(tickers))
                .filter(Price.date >= start)
                .filter(Price.date < end)
                .statement,
                session.bind,
            )
        results["symbol"].dropna().to_list()

        results["date"] = pd.to_datetime(results["date"])

        return (
            results.set_index("date")
            .pivot(columns="symbol", values="close")
            .ffill()
            .replace({np.nan: None})
        )

    @staticmethod
    def upsert(prices, init=False):
        # Convert column names to snake case.
        print(prices)
        prices = prices.reset_index()
        prices.columns = prices.columns.str.lower().str.replace(" ", "_")
        prices.date = prices.date.dt.date
        if init:
            with Session() as session:
                prices.to_sql(
                    "prices",
                    index=False,
                    con=session.connection(),
                    if_exists="replace" if init else "append",
                )
                session.commit()
        else:
            with Session() as session:
                # get list of fields making up primary key
                primary_keys = [key.name for key in inspect(Price).primary_key]

                values = prices.to_dict(orient="records")
                stmt = postgresql.insert(Price).values(values)

                # define dict of non-primary keys for updating
                update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}

                update_stmt = stmt.on_conflict_do_update(
                    index_elements=primary_keys,
                    set_=update_dict,
                )

                session.execute(update_stmt)
                session.commit()

    @staticmethod
    def tickers():
        with Session() as session:
            rows = session.execute("SELECT DISTINCT(symbol) FROM transactions;")
        list = [r[0] for r in rows]
        list.remove(None)
        return list

    @staticmethod
    def most_recent_date(symbol=None):
        with Session() as session:
            q = session.query(func.max(Price.date))
            if symbol:
                q = q.filter(Price.symbol == symbol)
            result = q.one()
        return result[0]


def init():
    Base.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_prices_symbol ON prices(symbol);"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);"
        )


init()
Session = sessionmaker(engine)
