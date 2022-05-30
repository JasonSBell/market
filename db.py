import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, Date, func, create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.inspection import inspect
from sqlalchemy.sql import text
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
    name = Column(String())
    ticker = Column(String(), unique=True, nullable=False)
    sector = Column(String())
    description = Column(String())
    shares_outstanding = Column(String())
    logo = Column(String())
    last_modified = Column(Date)

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
    def upsert_basic_info(ticker, name, logo, sector, description, shares_outstanding):
        with Session() as session:
            data = {
                "ticker": ticker,
                "name": name,
                "logo": logo,
                "sector": sector,
                "description": description,
                "shares_outstanding": shares_outstanding,
            }

            statement = text(
                """INSERT INTO companies (
                ticker,
                name,
                logo,
                sector,
                description,
                shares_outstanding
            ) VALUES (
                :ticker,
                :name,
                :logo,
                :sector,
                :description,
                :shares_outstanding
            ) ON CONFLICT (ticker) DO UPDATE SET 
                name=:name,
                logo=:logo,
                sector=:sector,
                description=:description,
                shares_outstanding=:shares_outstanding
            """
            )

            session.execute(statement, data)
            session.commit()

    @staticmethod
    def upsert_cik_info(ticker, cik, name):
        with Session() as session:
            data = {
                "ticker": ticker,
                "cik": cik,
                "name": name,
            }

            statement = text(
                """INSERT INTO companies (
                ticker,
                cik,
                name
            ) VALUES (
                :ticker,
                :cik,
                :name
            ) ON CONFLICT (ticker) DO UPDATE SET 
                cik=:cik
            """
            )

            session.execute(statement, data)
            session.commit()

    def bulk_upsert_cik_info(companies):
        with Session() as session:
            data = []
            for company in companies:
                data.append(
                    {
                        "ticker": company.ticker,
                        "cik": company.cik,
                        "name": company.name,
                    }
                )

            statement = text(
                """INSERT INTO companies (
                ticker,
                cik,
                name
            ) VALUES (
                :ticker,
                :cik,
                :name
            ) ON CONFLICT (ticker) DO UPDATE SET 
                cik=:cik
            """
            )

            for line in data:
                session.execute(statement, line)
            session.commit()


class Earnings(Base):
    __tablename__ = "earnings"
    date = Column(Date, primary_key=True, nullable=False)
    ticker = Column(String(), primary_key=True, nullable=False)

    @staticmethod
    def by_date(date):
        with Session() as session:
            query = (
                session.query(Earnings, Company.cik, Company.name)
                .join(
                    Company,
                    Earnings.ticker == Company.ticker,
                )
                .filter(Earnings.date == date)
                .order_by(Company.ticker)
            )
        return pd.read_sql(query.statement, session.bind)

    @staticmethod
    def list(tickers, before=None, after=None):
        with Session() as session:
            query = (
                session.query(Earnings, Company.cik, Company.name)
                .join(
                    Company,
                    Earnings.ticker == Company.ticker,
                )
                .filter(Earnings.ticker.in_(tickers))
            )

            if before:
                query = query.filter(Earnings.date < before)

            if after:
                query = query.filter(Earnings.date >= after)

            query = query.order_by(Earnings.date)
        return pd.read_sql(query.statement, session.bind)


class Dividend(Base):
    __tablename__ = "dividends"
    ex_date = Column(Date, primary_key=True, nullable=False)
    ticker = Column(String(), primary_key=True, nullable=False)
    dividend_rate = Column(Float())
    record_date = Column(Date)
    payment_date = Column(Date)
    announcement_date = Column(Date)

    @staticmethod
    def by_date(date):
        with Session() as session:
            query = (
                session.query(Dividend, Company.cik, Company.name)
                .join(
                    Company,
                    Dividend.ticker == Company.ticker,
                )
                .filter(Dividend.ex_date == date)
                .order_by(Company.ticker)
            )
        return pd.read_sql(query.statement, session.bind)

    @staticmethod
    def list(tickers, before=None, after=None):
        with Session() as session:
            query = (
                session.query(Dividend, Company.cik, Company.name)
                .join(
                    Company,
                    Dividend.ticker == Company.ticker,
                )
                .filter(Dividend.ticker.in_(tickers))
            )

            if before:
                query = query.filter(Dividend.ex_date < before)

            if after:
                query = query.filter(Dividend.ex_date >= after)

            query = query.order_by(Dividend.ex_date)
        return pd.read_sql(query.statement, session.bind)


class Split(Base):
    __tablename__ = "splits"
    date = Column(Date, primary_key=True, nullable=False)
    ticker = Column(String(), primary_key=True, nullable=False)
    ratio = Column(String())
    execution_date = Column(Date)
    announcement_date = Column(Date)

    @staticmethod
    def by_date(date):
        with Session() as session:
            query = (
                session.query(Split, Company.cik, Company.name)
                .join(
                    Company,
                    Split.ticker == Company.ticker,
                )
                .filter(Split.date == date)
                .order_by(Company.ticker)
            )
        return pd.read_sql(query.statement, session.bind)

    @staticmethod
    def list(tickers, before=None, after=None):
        with Session() as session:
            query = (
                session.query(Split, Company.cik, Company.name)
                .join(
                    Company,
                    Split.ticker == Company.ticker,
                )
                .filter(Split.ticker.in_(tickers))
            )

            if before:
                query = query.filter(Split.date < before)

            if after:
                query = query.filter(Split.date >= after)

            query = query.order_by(Split.date)
        return pd.read_sql(query.statement, session.bind)


class CongressionalTrade(Base):
    __tablename__ = "congressional_trades"
    transaction_date = Column(Date, primary_key=True, nullable=False)
    ticker = Column(String, primary_key=True, nullable=False)
    name = Column(String, primary_key=True, nullable=False)
    disclosure_date = Column(Date)
    body = Column(String)
    type = Column(String)
    amount = Column(String)
    comment = Column(String)
    url = Column(String)

    @staticmethod
    def by_date(date):
        with Session() as session:
            query = (
                session.query(CongressionalTrade, Company.cik, Company.name)
                .join(
                    CongressionalTrade,
                    CongressionalTrade.ticker == Company.ticker,
                )
                .filter(CongressionalTrade.transaction_date == date)
                .order_by(Company.ticker)
            )
        return pd.read_sql(query.statement, session.bind)

    @staticmethod
    def list(tickers, before=None, after=None, body=None):
        with Session() as session:
            query = (
                session.query(CongressionalTrade, Company.cik, Company.name)
                .join(
                    Company,
                    CongressionalTrade.ticker == Company.ticker,
                )
                .filter(CongressionalTrade.ticker.in_(tickers))
            )

            if before:
                query = query.filter(CongressionalTrade.transaction_date < before)

            if after:
                query = query.filter(CongressionalTrade.transaction_date >= after)

            if body:
                query = query.filter(CongressionalTrade.body == body)

            query = query.order_by(CongressionalTrade.transaction_date)
        return pd.read_sql(query.statement, session.bind)


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
    def company(ticker, start, end):

        with Session() as session:
            results = pd.read_sql(
                session.query(Price)
                .filter(Price.symbol == ticker)
                .filter(Price.date >= start)
                .filter(Price.date < end)
                .statement,
                session.bind,
            )
        results["symbol"].dropna().to_list()

        results["date"] = pd.to_datetime(results["date"])

        results.drop(columns=["symbol", "adj_close"], inplace=True)

        return results.set_index("date").ffill().replace({np.nan: None})

    @staticmethod
    def on(tickers, date):
        with Session() as session:
            results = pd.read_sql(
                session.query(Price)
                .filter(Price.symbol.in_(tickers))
                .filter(Price.date == date)
                .statement,
                session.bind,
            )
        return results

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
