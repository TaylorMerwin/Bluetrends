from keybert import KeyBERT


kw_model = KeyBERT()
# test out the keybert library with sample text

test_post = """
A lot of people seem to think following the U.S. Constitution is optional—like picking which shoes to wear in the morning. It’s not. If you’re within the boundaries of the United States, the Constitution applies to you.
"""

# Extract keywords
keywords = kw_model.extract_keywords(test_post, keyphrase_ngram_range=(1, 1), stop_words='english')
print(keywords)