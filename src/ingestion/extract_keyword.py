# src/ingestion/extract_keyword.py
from keybert import KeyBERT


kw_model = KeyBERT()
# test out the keybert library with sample text

test_post = """
#Easter 🐰 #Ostara 🥚 
Easter Bunny hops with great cheer, 🐰  
Hiding chocolate eggs far and near. 🍫🥚  
Under bushes and trees,  
In the soft springtime breeze,  
Spreading happiness year after year!🌷🌞
"""
# Extract keywords
keywords = kw_model.extract_keywords(test_post, keyphrase_ngram_range=(1, 1), stop_words='english')
print(keywords)