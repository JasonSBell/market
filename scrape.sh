source .venv/bin/activate

tickers=$(curl -s "https://allokate.ai/api/market/earnings?date=2022-08-23" | jq -r ".data[].ticker")

for ticker in $tickers; do 
    python sickle.py company $ticker;
    sleep $((10 + $(($RANDOM % 5))))
done