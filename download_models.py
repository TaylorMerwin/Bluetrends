from huggingface_hub import snapshot_download

# 1) sentiment model
snapshot_download(
    repo_id="cardiffnlp/twitter-roberta-base-sentiment-latest",
    cache_dir="models/twitter-roberta-base-sentiment-latest",
    library_name="my-bluetrends-download",
)

# 2) sentence-transformers model
snapshot_download(
    repo_id="sentence-transformers/all-MiniLM-L6-v2",
    cache_dir="models/all-MiniLM-L6-v2",
    library_name="my-bluetrends-download",
)

path = snapshot_download(
    repo_id="cardiffnlp/twitter-roberta-base-sentiment-latest",
    cache_dir="models/twitter-roberta-base-sentiment-latest",
    library_name="my-download")

print(f"Downloaded sentiment model to: {path}")