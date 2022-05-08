# Server

Server-side search logic focuses on minimizing latency. SQL queries are batched whenever possible and cached in memory. To support concurrent search requests, we use HikariCP connection pool.
By default, top 200 ranked results are returned for each query. Because extracting excerpts are expensive, document metadata are fetched lazily and client has the option to request a page of 10 results out of 200.

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