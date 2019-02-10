# data_warehouse_tutorial
## Overview
This repository is hosted by [@hayata_yamamoto](https://twitter.com/hayata_yamamoto?lang=ja).

The purpose is sharing source codes are shown in my article on note.mu

## Details 
- [GUI import](https://note.mu/hayata_yamamoto/n/n28643a077ded)
- [Python job handling](https://note.mu/hayata_yamamoto/n/n9623a254fea0)

## Structure
```text
data_warehouse_tutorial/
├── README.md
├── data
│   └── raw
│       ├── application_train.csv
│       ├── bureau.csv
│       ├── bureau_balance.csv
│       ├── credit_card_balance.csv
│       ├── installments_payments.csv
│       ├── pos_cash_balance.csv
│       ├── previous_application.csv
│       └── sample_submission.csv
└── src
    ├── cmd
    │   ├── bigquery
    │   └── mysql
    │       └── import_csv.py
    └── model
        ├── bigquery
        │   └── bigquery.py
        └── mysql
            ├── create_database.sql
            └── load.py
```