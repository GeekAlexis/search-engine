# CIS 555 Team Project

Our search engine is highly optimized with server-side caching and concurrent query support.
Document ranking incorporates BM25 and PageRank for high-quality retrieval.
Excerpts with highlighted hits are loaded dynamically and shown on the result page.
Web UI supports autocomplete and integrates search results from News and Yelp webservices.
Document index is built by distributed indexer/PageRank/crawler and stored in RDS (PostgreSQL). 

## Tech

- [ReactJS](https://reactjs.org/)
- [MUI](https://mui.com/)
- [Spark Java](https://sparkjava.com/)
- [News API](https://newsapi.org/)
- [Yelp Fushion API](https://www.yelp.com/developers/documentation/v3/get_started)
- [IP Geolocation API](https://ip-api.com/)

## Run Search Engine

### Server
The configuration `src/main/resources/config.properties` that contains API keys and database username/password is not provided.

```sh
cd server
mvn clean install
mvn exec:java {port}
```

### Client
```sh
cd client
npm i
npm start
```

## Install AWS CLI

```sh
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

## Run Indexer on EMR

### Pacakge into JAR
```sh
cd indexer
mvn clean install
aws s3 cp target/indexer-1.0-SNAPSHOT-launcher.jar s3://<YOUR S3 BUCKET>/indexer-1.0-SNAPSHOT-launcher.jar
```

### Launch EMR-6.6.0
Configure instances to use Java 11. 
Submit step and specify arguments: `s3://<YOUR S3 BUCKET>/in/ s3://<YOUR S3 BUCKET>/out/ temp/`

### Upload Index to RDS
```sh
cd indexer
aws s3 cp s3://<YOUR S3 BUCKET>/out/ out/ --recursive
cat out/part* > merged-index

mvn exec:java merged-index
```

### Team members
- Yukai Yang (yukaiy)
- Jiyi Ming (sylviaji)
- Hongyu Zhang (hz53)
- Yuchen Ding (ycding)
