tickers=$(curl -s "https://app.allokate.ai/api/market/earnings" | jq -r ".data[].ticker")

echo $tickers

for ticker in $tickers; do 
    python sickle.py company $ticker;
    sleep $((10 + $(($RANDOM % 5))))
done
