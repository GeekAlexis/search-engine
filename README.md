# CIS 555 Team Project

Our search engine is highly optimized with server-side caching and concurrent query support.
Document ranking incorporates BM25 and PageRank for high-quality retrieval.
Excerpts with highlighted hits are loaded dynamically and shown on the result page.
Web UI supports autocomplete and integrates search results from News and Yelp webservices.
Document index and metadata are built by distributed crawler/indexer/PageRank analysis and stored in RDS (PostgreSQL). 

## Tech

- [ReactJS](https://reactjs.org/)
- [MUI](https://mui.com/)
- [Spark Java](https://sparkjava.com/)
- [News API](https://newsapi.org/)
- [Yelp Fushion API](https://www.yelp.com/developers/documentation/v3/get_started)
- [IP Geolocation API](https://ip-api.com/)

## Quick Start

### Server
Specify `src/main/resources/config.properties` that contains your API keys and database credentials (not provided).

```ini
db.url=jdbc:postgresql://database.abcdefg.us-east-1.rds.amazonaws.com:1234/postgresdb
db.user=username
db.pass=password
news.apiKey=abcdefghijk
yelp.apiKey=abcdefghijk
```

```sh
cd server
mvn clean install
mvn exec:java
```

### Client
```sh
cd client
npm i
npm start
```

## Precomputed Components
- [Indexer](indexer/README.md)
- [PageRank](pagerank/pagerank/README.md)
- [Weblinkgraph](pagerank/weblinkgraph/README.md)
- [Crawler](crawler/README.md)

### Team members
- Yukai Yang (yukaiy)
- Jiyi Ming (sylviaji)
- Hongyu Zhang (hz53)
- Yuchen Ding (ycding)
