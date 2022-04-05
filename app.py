import json
import pandas as pd
import numpy as np
import datetime
from flask import Flask, request
from flask.json import jsonify, JSONEncoder
from flask_cors import CORS
from prometheus_flask_exporter import PrometheusMetrics
from pypfopt.expected_returns import mean_historical_return
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier


from config import config
import db


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


app = Flask(__name__)
app.json_encoder = CustomJSONEncoder
app.config["JSON_SORT_KEYS"] = False
CORS(app, supports_credentials=True)
metrics = PrometheusMetrics(app)
metrics.info("market", "Market API", version="0.1.0")


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):

        if isinstance(obj, datetime.datetime):
            return str(obj.isoformat())

        if isinstance(obj, datetime.date):
            return str(obj.isoformat())

        return json.JSONEncoder.default(
            self, obj
        )  # default, if not Delivery object. Caller's problem if this is not serialziable.


app.json_encoder = JSONEncoder


@app.route("/api/ping")
def ping():
    return jsonify({"message": "pong"})


@app.route("/api/market/earnings")
def earnings():
    args = request.args
    date = args.get("date", datetime.date.today())

    if isinstance(date, str):
        try:
            date = datetime.datetime.fromisoformat(date).date()
        except Exception as e:
            return (
                jsonify(
                    {
                        "error": '"date" must be a valid date string i.e. "YYYY-MM-DD"',
                    }
                ),
                400,
            )

    df = db.Earnings.by_date(date)

    return jsonify(
        {
            "date": date,
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records"),
        }
    )


@app.route("/api/market/dividends")
def dividends():
    args = request.args
    date = args.get("date", datetime.date.today())

    if isinstance(date, str):
        try:
            date = datetime.datetime.fromisoformat(date).date()
        except Exception as e:
            return (
                jsonify(
                    {
                        "error": '"date" must be a valid date string i.e. "YYYY-MM-DD"',
                    }
                ),
                400,
            )

    df = db.Dividend.by_date(date)

    return jsonify(
        {
            "date": date,
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records"),
        }
    )


@app.route("/api/market/splits")
def splits():
    args = request.args
    date = args.get("date", datetime.date.today())

    if isinstance(date, str):
        try:
            date = datetime.datetime.fromisoformat(date).date()
        except Exception as e:
            return (
                jsonify(
                    {
                        "error": '"date" must be a valid date string i.e. "YYYY-MM-DD"',
                    }
                ),
                400,
            )

    df = db.Split.by_date(date)
    return jsonify(
        {
            "date": date,
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records"),
        }
    )


@app.route("/api/market/activity")
def activity():
    args = request.args
    tickers = args.get("tickers", [])
    before = args.get("before", None)
    after = args.get("after", datetime.date.today() - datetime.timedelta(days=30))

    if isinstance(tickers, str):
        tickers = tickers.upper().split(",")

    if len(tickers) < 1:
        return (
            jsonify(
                {
                    "error": '"tickers" is a required query parameter',
                }
            ),
            400,
        )

    if isinstance(before, str):
        try:
            before = datetime.datetime.fromisoformat(before)
        except Exception as e:
            return (
                jsonify(
                    {
                        "error": '"before" must be a valid date string i.e. "YYYY-MM-DD"',
                    }
                ),
                400,
            )

    if isinstance(after, str):
        try:
            after = datetime.datetime.fromisoformat(after)
        except Exception as e:
            return (
                jsonify(
                    {
                        "error": '"after" must be a valid date string i.e. "YYYY-MM-DD"',
                    }
                ),
                400,
            )

    earnings = db.Earnings.list(tickers, before=before, after=after)
    splits = db.Split.list(tickers, before=before, after=after)
    dividends = db.Dividend.list(tickers, before=before, after=after)

    return jsonify(
        {
            "tickers": tickers,
            "before": before,
            "after": after,
            "earnings": earnings.to_dict(orient="records"),
            "dividends": dividends.to_dict(orient="records"),
            "splits": splits.to_dict(orient="records"),
        }
    )


@app.route("/api/market/prices")
def price():
    args = request.args
    tickers = args.get("tickers", "").upper().split(",")
    start = args.get("start", datetime.datetime.now() - datetime.timedelta(days=30))
    end = args.get("end", datetime.datetime.now())

    if len(tickers) < 1:
        return (
            jsonify(
                {
                    "error": '"tickers" is a required query parameter',
                }
            ),
            400,
        )

    df = db.Price.get(tickers=tickers, start=start, end=end).reset_index()
    df["date"] = df["date"].map(lambda x: x.isoformat())

    return jsonify(
        {
            "tickers": tickers,
            "start": start,
            "end": end,
            "columns": df.columns.tolist(),
            "data": df.values.tolist(),
        }
    )


@app.route("/api/market/tickers")
def tickers():
    companies = db.Company.list()
    return jsonify(
        [{"cik": c.cik, "ticker": c.ticker, "name": c.name} for c in companies]
    )


@app.route("/api/market/performance")
def performance():
    args = request.args
    tickers = args.get("tickers", "").upper().split(",")
    shares = args.get("shares", ",".join([str(1 / len(tickers))] * len(tickers))).split(
        ","
    )
    start = args.get("start", datetime.datetime.now() - datetime.timedelta(days=365))
    end = args.get("end", datetime.datetime.now())
    frequency = args.get("frequency", "M").upper()

    if len(tickers) < 2:
        return (
            jsonify(
                {
                    "error": '"tickers" must be a comma seperated list greater containing at least 2 values',
                }
            ),
            400,
        )

    if len(tickers) != len(shares):
        return (
            jsonify(
                {
                    "error": 'the length of "shares" must be the same as the length of "tickers"',
                }
            ),
            400,
        )

    if frequency not in ["M", "D", "W"]:
        return (
            jsonify(
                {
                    "error": '"frequency" must be one of ["M", "D", "W"]',
                }
            ),
            400,
        )

    try:
        shares = [float(s) for s in shares]
    except:
        return (
            jsonify(
                {
                    "error": '"shares" must be a list of numbers',
                }
            ),
            400,
        )

    shares = pd.Series(data=shares, index=tickers, name="shares")

    if (shares < 0).any():
        return (
            jsonify(
                {
                    "error": "shares must be greater than or equal to 0",
                }
            ),
            400,
        )

    p = db.Price.get(tickers=tickers, start=start, end=end)

    mu = mean_historical_return(p)
    S = CovarianceShrinkage(p).ledoit_wolf()
    ef = EfficientFrontier(mu, S)
    ef.max_sharpe()
    cleaned_weights = ef.clean_weights()
    expected, volatility, sharpe = ef.portfolio_performance(verbose=True)

    p = p.resample(frequency).apply(lambda x: x[-1])

    value = p * shares
    portfolio_returns = value.sum(axis=1).pct_change().iloc[1:]

    returns = p.pct_change().iloc[1:, :]
    df = pd.DataFrame()
    df["mean"] = returns.mean()
    df["std"] = returns.std()

    portfolio = {
        "return": portfolio_returns.mean(),
        "std": portfolio_returns.std(),
    }

    returns["Portfolio"] = portfolio_returns
    returns.index = returns.index.strftime("%Y-%m-%d")

    df = df.replace({np.nan: None})
    returns = returns.replace({np.nan: None})
    value = value.replace({np.nan: None})
    return jsonify(
        {
            "tickers": tickers,
            "start": start,
            "end": end,
            "frequency": frequency,
            "optimal": {
                "expected": expected,
                "volatility": volatility,
                "sharpe": sharpe,
                "weights": cleaned_weights,
            },
            "portfolio": portfolio,
            "positions": df.to_dict(orient="index"),
            "returns": returns.reset_index().to_dict(orient="records"),
            "value": value.sum(axis=1)
            .rename("value")
            .reset_index()
            .to_dict(orient="records"),
        }
    )


@app.route("/api/market/<ticker>")
def info(ticker):
    c = db.Company.get(ticker.upper())

    if c == None:
        return jsonify({"error": f'no company found with ticker "{ticker}"'}), 404

    return jsonify(
        {
            "ticker": c.ticker,
            "cik": c.cik,
            "name": c.name,
        }
    )


if __name__ == "__main__":
    app.run(port=config.port, host="0.0.0.0")
