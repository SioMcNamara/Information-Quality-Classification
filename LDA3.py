import mysql.connector
from nltk import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from gensim import corpora, models
from collections import defaultdict
import gensim
import sys

db = mysql.connector.connect(user='root', password='irld7812',
                              host='localhost',
                              database='stocktwits')

cursor = db.cursor()

# execute SQL query using execute() method.
sql = ("SELECT id, Body from 2015a")


cursor.execute(sql)
# Fetch all the rows in a list of lists.
(id, results) =zip(* cursor.fetchall())



#print results[:5]
'''
stocks=[]
for row in results:
  stocks.append(row)
'''
#print stocks[:5]

tokenizer = RegexpTokenizer(r'\w+')
en_stop = get_stop_words('en')
en2_stop = ['quot', 'amp', 'don', 'stocktwits', 'aapl', r'@\w+', 'stockcat01', 'apple', 'put', 'bit', 'will', 's_evan', 'get', '$appl', '$stocktwit', 'nyorka', 'dilsam', 'one', 'see', 'coolhobiecat', 'go', 'just', 'xa0', 'ep', 'use']
p_stemmer = PorterStemmer()

tweets = []

for word in results:
	tokens = tokenizer.tokenize(word)
	stopped_tokens = [word for word in tokens if not word in en_stop and word not in en2_stop and len(word) >=3]
	stemmed_tokens = [p_stemmer.stem(word) for word in stopped_tokens]
	tweets.append(stemmed_tokens)



dictionary = corpora.Dictionary(tweets)
corpus = [dictionary.doc2bow(tweet) for tweet in tweets]
ldamodel = models.ldamodel.LdaModel(corpus, num_topics=10, id2word= dictionary, passes = 20)


print(ldamodel.print_topics(num_topics=10, num_words=5))


mixture = [(ldamodel[x]) for x in corpus]
topics = []
for line in mixture:
	topics.append(max(line,key=lambda item:item[1]))

#print topics

res_list = zip(id, [x[0] for x in topics])

#print res_list[:5]


#query = "UPDATE 2015a SET TOPIC = %s WHERE id = %s"
for row in res_list:

	QUERY = "UPDATE 2015A SET TOPIC = "+str(row[1])+" WHERE id = "+str(row[0])
	print QUERY
	cursor.execute(QUERY)
	db.commit()

db.close()
