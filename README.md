# Big Data Analytics with Knowledge Graphs and NoSQL

Three big data projects on document databases, graph databases and knowledge graphs, completed as coursework at Macquarie University (2026).

| Project | What it does | Main tools |
|---|---|---|
| [Knowledge Lake](knowledge-lake/) | Extracts entities from news articles, links them to Wikidata and builds a knowledge graph | Python, spaCy, Wikidata API, Neo4j |
| [Social media analytics](mongodb-twitter/) | Cleans 10,000 tweets into MongoDB and detects trends and coordinated behaviour | Python, MongoDB, aggregation pipelines |
| [Fraud detection graph](neo4j-fraud-graph/) | Models a payments system as a graph and finds fraud patterns | Neo4j, Cypher, Graph Data Science |

No datasets are included in this repository. See [Data](#data) for where each dataset comes from.

---

## 1. Knowledge Lake

A pipeline that turns unstructured news text into a knowledge graph.

- **Entity extraction:** people, organisations and places are extracted from a stratified sample of 200 BBC News articles with spaCy, then cleaned with rules (titles, possessives, aliases such as *Britain* → *UK*, surname expansion).
- **Relationships:** entities mentioned in the same article are linked, weighted by the number of shared articles.
- **Entity linking:** the 50 most frequent entities are linked to Wikidata. A context-aware method, which scores candidates by entity type and article topic, raised linking accuracy from **78% to 88%** compared with taking the first search result.
- **Graph export:** articles, entities, mentions and co-occurrences are exported as CSV files for import into Neo4j.

**Run:** open `knowledge-lake/knowledge_lake_data_prep.ipynb` in Google Colab and follow the steps in the notebook. Add your own contact email in Step 1 (Wikimedia asks API users to identify themselves).

## 2. Social media analytics with MongoDB

A curation pipeline and analytical queries over a 10,000-tweet dataset.

- **`ingest_clean.py`:** reads raw JSON in two different formats, normalises the schema, adds derived fields (hour of day, weekday, cleaned text), removes duplicate tweets (map and reduce steps), splits users into their own collection and creates indexes.
- **`content_analytics.py`:** aggregation pipelines for emerging hashtags, coordinated link bursts and repeated near-duplicate posts.
- **`coordination_detection.py`:** detects possible sock-puppet accounts (shared attributes and hashtags) and cross-account promotion chains, using self-joins and time-bounded lookups.

**Run:**
```bash
pip install -r requirements.txt
python mongodb-twitter/ingest_clean.py --help
```
All scripts connect to `mongodb://localhost:27017` by default; use `--mongo-uri` to change it.

## 3. Fraud detection with Neo4j

A graph model of a payments system with clients, sellers, purchases and transfers.

- **Data model (`01_import_and_model.cypher`):** constraints and indexes; identifiers (email, phone, tax file number) stored as shared nodes so linked accounts connect directly; purchases and transfers modelled as subtypes of a common transaction type; provenance nodes for each source file; time-ordered transaction chains per client.
- **Fraud queries (`02_fraud_queries.cypher`):** top spenders in a time window, largest negative cashflow and money-mule patterns.
- **Graph data science (`03_graph_data_science.cypher`):** Weakly Connected Components and Louvain to find clusters of linked accounts, PageRank and betweenness centrality to find key routing accounts, and Node Similarity to predict hidden links between clients.

**Run:** in Neo4j Desktop (5.x) with the Graph Data Science plugin, run the files in order in Neo4j Browser.

---

## Data

The datasets are not redistributed here.

- **BBC News:** Greene, D. and Cunningham, P. 2006. *Practical Solutions to the Problem of Diagonal Dominance in Kernel Document Clustering.* ICML 2006. Available from the authors' website for research use.
- **Wikidata:** queried live through the public API (CC0).
- **Twitter and payments datasets:** provided for coursework and not publicly available. The payments data is synthetic.

## Requirements

- Python 3.10+, Google Colab (for the Knowledge Lake notebook)
- MongoDB 5.0+
- Neo4j 5.x with the Graph Data Science library

## Author

Anousha Shariati, Macquarie University
