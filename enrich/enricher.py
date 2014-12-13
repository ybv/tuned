import nltk
from pymongo import MongoClient

c = MongoClient("mongodb://tuned:tuned@ds033750.mongolab.com:33750/tuned-db")
db = c['tuned-db']

def get_all_content():
    all_content = ' '.join([p['text'] for p in db.articles.find()])
    return all_content

def get_all_content_as_list():
    all_content = [p['text'] for p in db.articles.find()]
    return all_content

if __name__=="__main__":
    cont = get_all_content()
    tokens = nltk.word_tokenize(cont)
    text = nltk.Text(tokens)
    fdist = text.vocab()
    Q = ['world']
    print [w for  w in fdist.keys()[:100] if w.lower() not in nltk.corpus.stopwords.words('english') ]



        
    


