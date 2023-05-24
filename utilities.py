import re
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk
import logging
import random

nltk.download('stopwords')
nltk.download('wordnet')

logging.basicConfig(level=logging.INFO)

def clean_text(text):
    logging.info("Cleaning text...")  # Add logging
    text = text.lower()
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
    words = re.findall(r'\w+', text)
    words = [lemmatizer.lemmatize(word) for word in words if word not in stop_words]
    text = ' '.join(words)
    logging.info("Text cleaning completed.")  # Add logging
    return text

def random_user_agent():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:86.0) Gecko/20100101 Firefox/86.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
    ]
    return random.choice(user_agents)

ignored_extensions = [
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.mp4', '.mov', '.avi', '.mkv', '.wmv',
    '.flv', '.mpg', '.mpeg', '.m4v', '.mp3', '.wav', '.ogg', '.m4a', '.zip', '.rar',
    '.7z', '.tar', '.gz', '.bz2', '.xlsx', '.xls', '.csv', '.json', '.xml', '.doc', '.docx',
    '.ppt', '.pptx', '.txt', '.log', '.iso', 'demo', '.pdf', '.svg', '.ico', '.ttf', '.woff', '.woff2'
]