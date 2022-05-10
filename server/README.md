# Server

Server-side search logic focuses on minimizing latency. SQL queries are batched whenever possible and cached in memory. To support concurrent search requests, we use HikariCP connection pool.
By default, top 100 ranked results are returned for each query. Because extracting excerpts are expensive, document metadata are fetched lazily and client has the option to request a page of 10 results out of 100.

## Source 
```
src/main
├── java
│   └── edu
│       └── upenn
│           └── cis
│               └── cis455
│                   ├── Server.java
│                   ├── handlers
│                   │   ├── NewsHandler.java
│                   │   ├── SearchHandler.java
│                   │   └── YelpHandler.java
│                   └── search
│                       ├── DocumentData.java
│                       ├── RankScore.java
│                       ├── Retrieval.java
│                       ├── RetrievalResult.java
│                       ├── TermOccurrence.java
│                       └── TokenizerConfig.java
└── resources
    ├── config.properties
    └── log4j2.yaml
```

## Run server
Specify `src/main/resources/config.properties` that contains your API keys and database credentials (not provided).

```ini
db.url=jdbc:postgresql://host:port/database
db.user=username
db.pass=password
news.apiKey=abcdefghijk
yelp.apiKey=abcdefghijk
```

```sh
mvn clean install
mvn exec:java {port}
```