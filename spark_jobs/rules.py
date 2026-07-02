from pyspark.sql.functions import col, trim


def equity_rules(df):

    df = df.withColumn(
        "instrument",
        trim(col("instrument"))
    )

    good = df.filter(

        (col("trade_id").isNotNull()) &
        (col("instrument") != "") &
        (col("quantity") > 0) &
        (col("price") > 0) &
        (col("trade_date").isNotNull())

    )

    bad = df.subtract(good)

    return good, bad


def bond_rules(df):

    good = df.filter(

        (col("trade_id").isNotNull()) &
        (col("isin").isNotNull()) &
        (col("isin") != "INVALID") &
        (col("maturity_date").isNotNull()) &
        (col("face_value") > 0) &
        (col("yield_rate") > 0)

    )

    bad = df.subtract(good)

    return good, bad


def commodity_rules(df):

    good = df.filter(

        (col("contract_id").isNotNull()) &
        (col("product_code").isNotNull()) &
        (col("product_code") != "") &
        (col("expiry_date").isNotNull()) &
        (col("quantity") > 0) &
        (col("price") > 0)

    )

    bad = df.subtract(good)

    return good, bad