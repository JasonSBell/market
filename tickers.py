import requests


def fetch_tickers():
    """Fetch a list of companies registered with the SEC returning their name, ticker, and CIK number."""

    res = requests.get("https://www.sec.gov/files/company_tickers.json")
    res.raise_for_status()

    companies = res.json().values()

    return list(
        map(
            lambda c: {
                "cik": c["cik_str"],
                "ticker": c["ticker"],
                "name": c["title"],
            },
            companies,
        )
    )


if __name__ == "__main__":
    import db

    tickers = fetch_tickers()
    db.Company.upsert(tickers)
