from pprint import pprint
from types import SimpleNamespace
import argparse
import datetime
import requests
import pandas as pd
import yfinance as yf

import db


def get_companies_registered_with_the_sec():
    """Fetch a list of companies registered with the SEC returning their name, ticker, and CIK number."""

    res = requests.get("https://www.sec.gov/files/company_tickers.json")
    res.raise_for_status()

    companies = res.json().values()

    return list(
        map(
            lambda c: SimpleNamespace(
                **{
                    "cik": c["cik_str"],
                    "ticker": c["ticker"],
                    "name": c["title"],
                }
            ),
            companies,
        )
    )


def update_cik_info():
    companies = get_companies_registered_with_the_sec()
    db.Company.bulk_upsert_cik_info(companies)


def get_basic_company_info(ticker):
    t = yf.Ticker(ticker)
    data = t.info
    return SimpleNamespace(
        **{
            "ticker": ticker,
            "name": data["shortName"],
            "logo": data["logo_url"],
            "description": data["longBusinessSummary"],
            "sector": data["sector"],
            "shares_outstanding": data["sharesOutstanding"],
        }
    )


def update_basic_company_info(ticker):
    info = get_basic_company_info(ticker)

    db.Company.upsert_basic_info(
        info.ticker,
        info.name,
        info.logo,
        info.sector,
        info.description,
        info.shares_outstanding,
    )

    return info


def download_pricing_data(tickers, **kwargs):
    """Downloads data from Yahoo Fiance returning a series of rows where each
    row contains the information for a given symbol on a given date. For example:

                Symbol     Adj Close       Close        High         Low        Open       Volume
    Date
    2021-12-01       A    148.210007  148.210007  152.850006  148.089996  151.119995    1800900.0
    2021-12-01     AAL     16.280001   16.280001   18.240000   16.260000   17.940001   82030100.0
    2021-12-01    AAPL    164.770004  164.770004  170.300003  164.529999  167.479996  152052500.0
    """
    kwargs["tickers"] = tickers
    kwargs["interval"] = "1d"
    kwargs["group_by"] = "Ticker"
    df = yf.download(**kwargs)
    if len(tickers) > 1:
        df = df.stack(level=0).rename_axis(["Date", "Symbol"]).reset_index(level=1)
    else:
        df.insert(0, "Symbol", tickers[0])
    return df


def combine_pricing_data(df1, df2):
    """Combines two pandas Dataframe objects containing historical pricing
    data. This function appends df2 to df1 prefering to keep entries from df2
    when duplicates are found."""
    return (
        df1.append(df2)
        .reset_index()
        .drop_duplicates(subset=["Date", "Symbol"], keep="last")
        .set_index("Date")
    )


def download_incremental_pricing_data():
    # Determine where that last download left off.
    tickers = db.Price.tickers()
    start = db.Price.most_recent_date()

    # Download the latest incremental data.
    df = download_pricing_data(tickers, start=start)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")

    subcommand_parser = parser.add_subparsers(dest="subcommand")

    prices_parser = subcommand_parser.add_parser("prices")
    prices_parser.add_argument("--incremental", action="store_true")
    prices_parser.add_argument("--init", action="store_true")
    prices_parser.add_argument("--ticker", dest="tickers", action="append")
    prices_parser.add_argument("--dry-run", dest="dry_run", action="store_true")

    company_parser = subcommand_parser.add_parser("company")
    company_parser.add_argument("ticker")

    args = parser.parse_args()

    def log(*msg):
        if args.verbose:
            print(*msg)

    if args.subcommand == "prices":
        log("Sickle executed at", datetime.datetime.today())

        df = None
        if args.incremental:
            print("Running in incremental mode")
            df = download_incremental_pricing_data()

        else:
            tickers = args.tickers if args.tickers else []
            if len(tickers) < 1:
                print(
                    "you must specify a list of symbols if not running in incremental mode"
                )
                exit(1)

            tickers = [t.upper() for t in tickers]

            log(f"Downloading data for {len(tickers)} symbol: {tickers}")

            df = download_pricing_data(tickers, period="max")

        if args.dry_run:
            print(df)
        else:
            db.Price.upsert(df, init=args.init)
            log(f"Data written to db")

    elif args.subcommand == "company":
        ticker = args.ticker.upper()
        log(f"Fetching company data for", ticker)
        info = update_basic_company_info(ticker)
        pprint(dict(vars(info)))

    else:
        pass


if __name__ == "__main__":
    main()
