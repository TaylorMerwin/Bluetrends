# src/ingestion/extract_keyword.py
from keybert import KeyBERT


kw_model = KeyBERT()
# test out the keybert library with sample text

test_post = """
Spark is awesome! I love it so much! spark is the best thing since pandas!
"""
# Extract keywords
keywords = kw_model.extract_keywords(test_post, keyphrase_ngram_range=(1, 1), stop_words='english')
print(keywords)