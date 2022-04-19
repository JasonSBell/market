import db
import argparse
import datetime
import pandas as pd
import yfinance as yf


def download(tickers, **kwargs):
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


def combine(df1, df2):
    """Combines two pandas Dataframe objects containing historical pricing
    data. This function appends df2 to df1 prefering to keep entries from df2
    when duplicates are found."""
    return (
        df1.append(df2)
        .reset_index()
        .drop_duplicates(subset=["Date", "Symbol"], keep="last")
        .set_index("Date")
    )


def download_incremental_data():
    # Determine where that last download left off.
    tickers = db.Price.tickers()
    start = db.Price.most_recent_date()

    # Download the latest incremental data.
    df = download(tickers, start=start)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--ticker", dest="tickers", action="append")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    def log(*msg):
        if args.verbose:
            print(*msg)

    log("Sickle executed at", datetime.datetime.today())

    df = None
    if args.incremental:
        print("Running in incremental mode")
        df = download_incremental_data()

    else:
        tickers = args.tickers if args.tickers else []
        if len(tickers) < 1:
            print(
                "you must specify a list of symbols if not running in incremental mode"
            )
            exit(1)

        tickers = [t.upper() for t in tickers]

        log(f"Downloading data for {len(tickers)} symbol: {tickers}")

        df = download(tickers, period="max")

    if args.dry_run:
        print(df)
    else:
        db.Price.upsert(df, init=args.init)
        log(f"Data written to db")


if __name__ == "__main__":
    main()
