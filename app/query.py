import sys
import re
import math
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster


query_input = sys.argv[1]
if not query_input:
    print("No query provided")
    sys.exit(0)

query_terms = re.findall(r"[a-z]+", query_input.lower())
if not query_terms:
    print("No valid words in query")
    sys.exit(0)

cluster = Cluster(["cassandra-server"])
session = cluster.connect("search_engine")
corpus_row = session.execute("SELECT total_docs, avg_dl FROM corpus_stats WHERE id = 1").one()
if not corpus_row:
    print("Index not found in Cassandra")
    sys.exit(1)

N = corpus_row.total_docs
avg_dl = corpus_row.avg_dl

doc_lens = {}
for row in session.execute("SELECT doc_id, length FROM doc_lens"):
    doc_lens[row.doc_id] = row.length

postings_data = []
for term in set(query_terms):
    row = session.execute("SELECT df, postings FROM inverted_index WHERE word = %s", (term,)).one()
    if row:
        df = row.df
        for posting in row.postings.split(","):
            doc_id, tf = posting.split(":")
            postings_data.append((doc_id, (term, int(tf), df)))

cluster.shutdown()

if not postings_data:
    print(f"No matching documents found for: '{query_input}'")
    sys.exit(0)

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

b_N = sc.broadcast(N)
b_avg_dl = sc.broadcast(avg_dl)
b_doc_lens = sc.broadcast(doc_lens)
rdd_postings = sc.parallelize(postings_data)

def bm25_score(record):
    doc_id, (term, tf, df) = record
    N_val = b_N.value
    avg_dl_val = b_avg_dl.value
    doc_len = b_doc_lens.value.get(doc_id, avg_dl_val)

    k1 = 1.0
    b = 0.75
    idf = math.log(N_val / float(df))
    numerator = tf * (k1 + 1)
    denominator = k1 * ((1 - b) + b * (doc_len / avg_dl_val)) + tf
    score = idf * (numerator / denominator)

    return (doc_id, score)

rdd_scores = rdd_postings.map(bm25_score).reduceByKey(lambda a, b: a + b)

top_10 = rdd_scores.takeOrdered(10, key=lambda x: -x[1])
top_10_ids = [x[0] for x in top_10]

raw_docs_rdd = sc.textFile("hdfs://cluster-master:9000/input/data/part-*")

def extract_title(line):
    parts = line.split("\t")
    if len(parts) >= 2:
        return (parts[0], parts[1])
    return (None, None)

titles_rdd = raw_docs_rdd.map(extract_title).filter(lambda x: x[0] in top_10_ids)
titles_dict = dict(titles_rdd.collect())

print(f"=== BM25 for '{query_input}' ===")
for rank, (doc_id, score) in enumerate(top_10, 1):
    title = titles_dict.get(doc_id, "Unknown Title").replace("_", " ")
    print(f"{rank}. Score: {score:.4f}, Title: {title}, ID: {doc_id}")

sc.stop()
