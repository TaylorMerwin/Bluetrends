import pandas as pd
from transformers import pipeline


# Create a dataframe with text and sentiment columns

# initialize once
pipe = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
)


def sentiment_udf(texts: pd.Series) -> pd.DataFrame:

    texts_list = texts.tolist()
    results = pipe(texts_list)

    return pd.DataFrame({
        "text": texts_list,
        "sentiment_score": [result['score'] for result in results],
        "sentiment_label": [result['label'] for result in results],
    })



if __name__ == "__main__":
    sample = pd.Series(["I love this!", "This is terrible."])
    df_out = sentiment_udf(sample)
    print(df_out)