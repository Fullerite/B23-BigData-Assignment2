import sys


current_word = None
doc_tf = {}
total_docs = 0
total_length = 0
stats_printed = False


def emit_word_index(word, tf_dict):
    df = len(tf_dict.keys())
    postings = ",".join([f"{doc}:{tf}" for doc, tf in tf_dict.items()])
    print(f"INDEX\t{word}\t{df}\t{postings}")


for line in sys.stdin:
    parts = line.strip().split("\t")

    key = parts[0]
    if key == "META":
        meta_type = parts[1]
        if meta_type == "N":
            total_docs += int(parts[2])
        elif meta_type == "dl":
            doc_id = parts[2]
            length = int(parts[3])
            total_length += length
            print(f"LEN\t{doc_id}\t{length}")
        continue
        
    if not stats_printed:
        avg_dl = total_length / total_docs
        print(f"CORPUS\t{total_docs}\t{avg_dl}")
        stats_printed = True

    doc_id = parts[1]
    count = int(parts[2])
    if current_word == key:
        doc_tf[doc_id] = doc_tf.get(doc_id, 0) + count
    else:
        if current_word:
            emit_word_index(current_word, doc_tf)
        current_word = key
        doc_tf = {doc_id: count}

if current_word:
    emit_word_index(current_word, doc_tf)
