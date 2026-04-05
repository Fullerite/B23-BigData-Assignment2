import subprocess
from cassandra.cluster import Cluster


cluster = Cluster(['cassandra-server'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
session.set_keyspace("search_engine")
session.execute("CREATE TABLE IF NOT EXISTS corpus_stats (id int PRIMARY KEY, total_docs int, avg_dl float);")
session.execute("CREATE TABLE IF NOT EXISTS doc_lens (doc_id text PRIMARY KEY, length int);")
session.execute("CREATE TABLE IF NOT EXISTS inverted_index (word text PRIMARY KEY, df int, postings text);")
session.execute("TRUNCATE corpus_stats;")
session.execute("TRUNCATE doc_lens;")
session.execute("TRUNCATE inverted_index;")

cat = subprocess.Popen(["hdfs", "dfs", "-cat", "/indexer/index/part-*"], stdout=subprocess.PIPE)
corpus_statement = session.prepare("INSERT INTO corpus_stats (id, total_docs, avg_dl) VALUES (1, ?, ?)")
len_statement = session.prepare("INSERT INTO doc_lens (doc_id, length) VALUES (?, ?)")
index_statement = session.prepare("INSERT INTO inverted_index (word, df, postings) VALUES (?, ?, ?)")
count = 0
for line in cat.stdout:
    parts = line.decode("utf-8").strip().split("\t")

    row_type = parts[0]
    if row_type == "CORPUS":
        session.execute(corpus_statement, (int(parts[1]), float(parts[2])))
    elif row_type == "LEN":
        session.execute(len_statement, (parts[1], int(parts[2])))
    elif row_type == "INDEX":
        session.execute(index_statement, (parts[1], int(parts[2]), parts[3]))

    count += 1

cluster.shutdown()
print(f"Successfully loaded {count} entries to Cassandra")
