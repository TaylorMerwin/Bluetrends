#!/usr/bin/env bash
pip install --no-cache-dir sentence-transformers transformers torch
python3 - << 'EOF'
from sentence_transformers import SentenceTransformer
SentenceTransformer(
  "all-MiniLM-L6-v2",
  cache_folder="/root/.cache/huggingface"
)
EOF