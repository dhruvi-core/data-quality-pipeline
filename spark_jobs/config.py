DATASETS = {

    "equity": {
        "input": "/opt/spark/data/raw/equity/equity_trades.csv",
        "good": "/opt/spark/data/processed/good/equity",
        "bad": "/opt/spark/data/processed/bad/equity"
    },

    "bond": {
        "input": "/opt/spark/data/raw/bond/bond_positions.csv",
        "good": "/opt/spark/data/processed/good/bond",
        "bad": "/opt/spark/data/processed/bad/bond"
    },

    "commodity": {
        "input": "/opt/spark/data/raw/commodity/commodity_contracts.csv",
        "good": "/opt/spark/data/processed/good/commodity",
        "bad": "/opt/spark/data/processed/bad/commodity"
    }

}