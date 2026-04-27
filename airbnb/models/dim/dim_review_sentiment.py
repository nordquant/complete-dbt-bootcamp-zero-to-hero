from textblob import TextBlob

def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["textblob"]
    )

    reviews_df = dbt.ref("fct_reviews")

    df = reviews_df.to_pandas()

    df["SENTIMENT_SCORE"] = df["REVIEW_TEXT"].apply(get_sentiment)

    return df
