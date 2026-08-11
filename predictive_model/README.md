# Predictive model

This folder contains a Spark MLlib implementation for a dividend-capture
classifier built around the event-grain table from the M1 dividend-event
pipeline.

## What the model predicts

The script trains a binary classifier to predict whether an ex-dividend event
will produce a positive abnormal capture return:

- label = 1 when capture_ret_abn > 0
- label = 0 otherwise

That choice follows the project README, which frames capture_ret_abn as the
relevant profitability metric after removing the market's overnight move.

## Features used

The model uses a compact feature set from the existing grain table:

- dividend size and yield signals
- drop and return behaviour around the ex-date
- pre-event drift and volatility
- liquidity and trading activity
- simple categorical buckets for yield, size, and volatility

## How to run

Local development:

```bash
python predictive_model/spark_dividend_predictor.py --local --input predictive_model/sample_events.csv --output-dir predictive_model/results
```

Cluster / HDFS usage:

```bash
spark-submit --master yarn --deploy-mode client \
  --num-executors 4 --executor-memory 4g --executor-cores 2 \
  predictive_model/spark_dividend_predictor.py \
  --input /user/ms16965_nyu_edu/divcap/curated/div_event_grain \
  --output-dir /user/ms16965_nyu_edu/divcap/predictive_model/results
```

## Output

The script writes:

- a trained logistic regression model under the output directory
- a text summary with AUC and the most influential coefficients
- a predictions CSV directory with one row per scored event, including label, raw prediction, binary prediction, and probability
