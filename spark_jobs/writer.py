def save_dataframe(df, path):

    df.write.mode("overwrite").csv(
        path,
        header=True
    )