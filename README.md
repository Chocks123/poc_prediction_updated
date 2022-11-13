# dh_poc_prediction_framework_aws_glue_forecast_automl

This is a PoC to build a Machine Learning solution as a replacement for Dominro in the target state to predict target values for a use case. This PoC used AWS Glue jobs & Workflow along with Amazon Forecast(Auto ML)

Workflow Properties:

timestamp_format
yyyy-MM-dd

exchange_frequency_pair
UK_LSE_DAILY_

back_fill
zero

backtest_windows
1

inBucket
stock-pricedata-uk-lse-daily-in

incomingStockDatafile
stock/stock-pricedata-uk-lse-daily-in.csv

middle_fill
zero

aggregation_value
sum

dataset_frequency
D

forecast_horizon
40

outBucket
stock-forecast-uk-lse-daily-out
