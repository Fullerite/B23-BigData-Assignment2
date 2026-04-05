import re
import sys


for line in sys.stdin:
    parts = line.strip().split("\t")

    doc_id = parts[0]
    content = parts[2]

    words = re.findall(r"[a-z]+", content.lower())
    doc_len = len(words)

    print(f"META\tN\t1")
    print(f"META\tdl\t{doc_id}\t{doc_len}")

    for word in words:
        print(f"{word}\t{doc_id}\t1")
