from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("hdfs://cluster-master:9000/o.parquet")
n = 1200
df = df.select(["id", "title", "text"]).sample(fraction=n / df.count(), seed=0).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        if row["text"] is not None:
            cleaned_text = row["text"].replace("\n", " ").replace("\t", " ").replace("\r", " ")
        else:
            cleaned_text = ""
        f.write(cleaned_text)

df.foreach(create_doc)


def format_record(row):
    base_name = sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_")
    doc_id, doc_title = base_name.split("_", 1)

    if row["text"] is not None:
        cleaned_text = row["text"].replace("\n", " ").replace("\t", " ").replace("\r", " ")
    else:
        cleaned_text = ""

    return f"{doc_id}\t{doc_title}\t{cleaned_text}"


formatted_rdd = df.rdd.map(format_record)
formatted_rdd.repartition(1).saveAsTextFile("hdfs://cluster-master:9000/input/data")

spark.stop()
