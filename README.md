This project implements a distributed search engine using the Okapi BM25 ranking algorithm. It handles data preparation via PySpark, indexing via Hadoop MapReduce, and storage in Cassandra.

## How to Run

1. **Environment Setup**:
   Ensure Docker and Docker Compose are installed. Place the `o.parquet` file inside the `app/` folder.

2. **Start the Cluster**:
   ```bash
   docker compose up -d
   ```

3. **Automatic Execution**:
   The `docker-compose.yml` is configured to run `app.sh` as an entrypoint. This script will:
   - Start Hadoop/YARN services.
   - Build the Python virtual environment and install dependencies.
   - Prepare the data (sampling and formatting).
   - Run the MapReduce Indexer.
   - Load the results into Cassandra.
   - Run a sample query.

4. **Manual Search**:
   To run specific queries manually, enter the master node and use the search script:
   ```bash
   docker exec -it cluster-master bash
   source .venv/bin/activate
   ./search.sh "your query here"
   ```

## System Specifications
- **OS**: PopOS Linux
- **Resources**: 16GB RAM, 8-core CPU
- **Data**: ~1,172 Wikipedia documents sampled from `o.parquet`.
```
