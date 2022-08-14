import pandas as pd  # For Dataframe/JSON processing
import numpy as np  # For Matrix operation
import nltk  # Language Processing
from nltk.tokenize import word_tokenize
import re  # Regex
from gensim.parsing.preprocessing import STOPWORDS

df_review = pd.read_json(
    "C:\Coding\soal2 - clean and transform\de_skill_test_efishery\soal-2-clean-and-transform\soal2.json")
