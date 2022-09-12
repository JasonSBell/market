tickers=$(curl -s "https://allokate.ai/api/market/earnings" | jq -r ".data[].ticker")

for ticker in $tickers; do 
    python sickle.py company $ticker;
    sleep $((10 + $(($RANDOM % 5))))
done
