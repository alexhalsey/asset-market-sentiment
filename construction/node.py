#!/opt/local/bin/python3
import pandas as pd
import numpy as np
import re, itertools, sys, string
import csv

import gzip
import xml.etree.ElementTree as ET

import gensim
import nltk
from nltk.corpus import wordnet
from textblob import TextBlob, Word

import warnings
warnings.filterwarnings('ignore')

sno = nltk.stem.SnowballStemmer('english')

## TEXT CLEANING FUNCTIONS ################################################
def load_dict():
    fs_dictionary = pd.read_csv('path to stemmed dictionary').drop(['Unnamed: 0'], axis=1).dropna()

    # convert to dictionary for easier processing
    fs_dict = {}
    keys = list(fs_dictionary['Word'])
    for key in keys:
        fs_dict[key] = fs_dictionary.loc[fs_dictionary['Word']==key, 'Sentiment'].values[0]

    return fs_dict

def remove_punctuation(input_string):
    # Make a regular expression that matches all punctuation
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    # Use the regex
    return regex.sub(' ', input_string)

def preprocess(sentence):
  # expects sentence to be a string
  # simple preprocess function that tokenizes and clears punctuation
  outlist = gensim.utils.simple_preprocess(str(sentence), deacc=True)

  # filter out short words
  #return([word for word in outlist if word not in gensim.parsing.preprocessing.STOPWORDS and len(word) >=3 ]) 
  return([word for word in outlist if len(word) >=3 ])

fs_dict = load_dict()
def article_sentiment(token_list):
    total_count = len(token_list)    
    sentiment = 0
    match_list = []
    for token in token_list:
        if token in fs_dict:
            sentiment += fs_dict[token]
            match_list.append(token)
    return sentiment/total_count, match_list

negation_words = [
    "aint", "arent", "cannot", "cant", "couldnt", "darent", "didnt", "doesnt",
    "hadnt", "hasnt", "havent", "isnt", "mightnt", "mustnt", "neither", "don't","neednt",
    "never", "none", "nope", "nor", "not", "nothing", "nowhere", "no", "nobody",
    "oughtnt", "shant", "shouldnt", "uhuh", "wasnt", "werent", "without", "wont", "wouldnt", 
    "rarely", "seldom", "despite",  "hardly", "scarcely", "barely", "few", "little"
]
def article_sentiment_negation(token_list):
    total_count = len(token_list)    
    sentiment = 0
    match_list = []
    for i, token in enumerate(token_list):
        if token in fs_dict:
            sign = 1
            if set(token_list[max(0,i-3):min(i+3, len(token_list))]) & set(negation_words):
                sign = -1
            sentiment += fs_dict[token]*sign
            match_list.append(token)
    return sentiment/total_count

asset_list = ['Treasury', 'Treasuries', 'T-bill', 'risk-free', 'risk free', 'corporate bond', 'corporate spread', 'corporate earning', 'junk bond', 'junk-bond', 'investment-grade', 
                'investment grade', 'high-yield', 'high yield', 'fixed-income', 'fixed income', 'leveraged loan', 'secondary market', 'default rate', 'stock price', 'options price', 
                'options market', 'Dow Jones', 'DJIA', 'equity price', 'equity premium', 'equity premia', 'equity market', 'S&P 500', 'NASDAQ', 'VIX', 'real estate', ' CRE ', 'vacancy rate', 'house price', 
                'mortgage', 'borrowing cost', 'LIBOR', 'underwriting', 'capitalization rate', 'capitalization spread', 'market liquidity', 'bid-ask', 'risk appetite']

pairs = {"Treasuries": "Treasury",
         "T-bill": "Treasury",
         "risk-free": "risk free",
         "junk-bond": "junk bond",
         "investment-grade": "investment grade",
         "high-yield": "high yield",
         "fixed-income": "fixed income",
         "DJIA": "Dow Jones",
         "equity premium": "equity premia"
         }


def get_asset_words(body_of_text):
    word_list = []  
    for word in asset_list:
        if word.lower() in body_of_text.lower():
            if word in pairs:
                word_list.append(pairs[word])
            else:
                word_list.append(word)
    return word_list

def letter_freq(body_of_text):
  # returns proportion of the characters that are letters
  # used to filter out articles that are just big tables or lists of quotes
  split_words = [list(word) for word in body_of_text.split()]
  chars = [item for sublist in split_words for item in sublist]
  if len(chars) == 0:
      return -1
  chars = [char.isalpha() for char in chars]
  letter_freq = chars.count(True)/len(chars)
  return letter_freq   

def stem(text):
  # expects text to be a list
  result=[]

  for token in text:
    result.append(sno.stem(token))

  return result

def count_uncertainty(text):
    # expects text to be a list
    count = 0
    for token in text:
        if token in ['uncertain', 'uncertainti']:
            count +=1
    return count

def flatten(list):
    return [item for sublist in list for item in sublist]

def create_sentiment_index(infile):
    print(infile)
    print("reading in file")
    articles = pd.read_csv(infile)

    articles = articles.rename(columns={"newdate":"Timestamp"})

    articles = articles.drop_duplicates()

    print('stripping punctuation')
    articles['body'] = articles['body'].astype(str)
    articles['body'] = articles["body"].apply(lambda x: x.replace("\n", " "))
    articles['body'] = articles["body"].apply(lambda x: remove_punctuation(x).lower())

    articles = articles[articles['body'].apply(letter_freq) > 0.75]

    # save asset words
    articles['asset_words'] = articles['body_raw'].apply(get_asset_words)

    print('preprocessing and tokenizing')
    articles['body_tokenized'] = articles["body"].apply(preprocess)

    print('stemming')
    articles['body_stemmed'] = articles['body_tokenized'].apply(stem)

    print("computing sentiments")
    ## FS dictionary
    articles['FSSentiment'], articles['FS_words'] = zip(*articles['body_stemmed'].map(article_sentiment))
    ## FS dictionary with negation
    articles['FSSentiment_negations'] = articles['body_stemmed'].map(article_sentiment_negation)

    ## uncertainty counts
    articles['uncertainty_count'] = articles['body_stemmed'].apply(count_uncertainty)

    # drop articles that dont match dict
    fs_articles = articles[articles['FS_words'].apply(len) != 0]

    # drop the duplicates
    fs_articles = fs_articles.drop_duplicates(subset=['Timestamp', 'headline'], keep='last')
    fs_articles = fs_articles.drop_duplicates(subset=['Timestamp', 'body'], keep='last')

    fs_sentiment_distribution = fs_articles.copy(deep=True).drop(['asset_words'], axis=1)
    fs_sentiment_distribution_only = fs_sentiment_distribution.copy(deep=True)
    fs_sentiment_distribution_only = fs_sentiment_distribution_only[['Timestamp', 'FSSentiment', 'FSSentiment_negations']]

    sentiment_index = fs_articles[['Timestamp', 'FSSentiment', 'FSSentiment_negations', 'body_stemmed', 'uncertainty_count']]
    sentiment_index = sentiment_index.rename(columns={'FSSentiment':'Sentiment', 'FSSentiment_negations': 'Sentiment_neg'})

    # need for normalization later on
    sentiment_index['Count'] = 1
    sentiment_index['word_count'] = sentiment_index['body_stemmed'].apply(len)
    sentiment_index = sentiment_index.drop(['body_stemmed'], axis=1)
    sentiment_index['Timestamp'] = pd.to_datetime(sentiment_index['Timestamp'])

    sentiment_index['Month'] = sentiment_index['Timestamp'].dt.strftime('%m').apply(int)
    sentiment_index['Year'] = sentiment_index['Timestamp'].dt.strftime('%Y').apply(int)

    sentiment_index = sentiment_index.drop(['Timestamp'], axis=1)
    sentiment_index_monthly = sentiment_index.groupby(['Year', 'Month']).sum()
    sentiment_index_monthly = sentiment_index_monthly.reset_index()
    sentiment_index_monthly['YearMon'] = pd.to_datetime(sentiment_index_monthly.Year.astype(str) + '/' + sentiment_index_monthly.Month.astype(str) + '/01')

    sentiment_index_monthly['weighted_index'] = sentiment_index_monthly['Sentiment']/sentiment_index_monthly['Count'] * 100
    sentiment_index_monthly['weighted_index_neg'] = sentiment_index_monthly['Sentiment_neg']/sentiment_index_monthly['Count'] * 100
    sentiment_index_monthly['uncertainty_index'] = sentiment_index_monthly['uncertainty_count']/sentiment_index_monthly['Count'] * 100
    sentiment_index_monthly['avg_word_count'] = sentiment_index_monthly['word_count']/sentiment_index_monthly['Count'] 
    sentiment_index_monthly = sentiment_index_monthly.drop(['word_count'], axis=1)

    # compute disagreement
    disagreement = [x* 100 for x in sentiment_index.groupby(['Month', 'Year']).std()['Sentiment'].to_list()]
    disagreement_neg = [x* 100 for x in sentiment_index.groupby(['Month', 'Year']).std()['Sentiment_neg'].to_list()]
    sentiment_index_monthly['disagreement'] = disagreement
    sentiment_index_monthly['disagreement_neg'] = disagreement_neg

    # save FS words
    fs_words = flatten(fs_articles['FS_words'].to_list())

    fs_counts = {}
    for word in list(set(fs_words)):
        fs_counts[word] = fs_words.count(word)

    # save asset words
    asset_words = flatten(fs_articles['asset_words'].to_list())

    asset_counts = {}
    for word in list(set(asset_words)):
        asset_counts[word] = asset_words.count(word)

    return(sentiment_index_monthly, fs_counts, asset_counts, fs_sentiment_distribution, fs_sentiment_distribution_only)

print('here')
infile=f'{sys.argv[1]}/filtered_{sys.argv[3]}.csv'
outfile=f'{sys.argv[2]}/{sys.argv[3]}_sentiments.csv'

distr_file=f'{sys.argv[2]}/{sys.argv[3]}_sentiment_distribution.csv'
distr_file_only=f'{sys.argv[2]}/{sys.argv[3]}_sentiment_distribution_only.csv'

fs_words_file = f'{sys.argv[2]}/{sys.argv[3]}_fs_words.csv'
asset_words_file = f'{sys.argv[2]}/{sys.argv[3]}_asset_words.csv'

sentiments_df, fs_words, asset_counts, fs_sentiment_distribution, fs_sentiment_distribution_only = create_sentiment_index(infile)
sentiments_df.to_csv(outfile, index=False)
fs_sentiment_distribution.to_csv(distr_file, index=False)
fs_sentiment_distribution_only.to_csv(distr_file_only, index=False)

with open(fs_words_file, 'w') as f:
    for key in fs_words:
        f.write(f'{key}, {fs_words[key]}\n')


with open(asset_words_file, 'w') as f:
    for key in asset_counts:
        f.write(f'{key}, {asset_counts[key]}\n')


print("Hello There Apples")
