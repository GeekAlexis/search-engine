# Indexer

Yukai Yang (yukaiy@seas.upenn.edu)

An distributed indexer that builds an positional inverted index.
Indexer implemented as a Hadoop mapreduce job, which consists of a parser (mapper) and inverter (reducer).
Each parser tokenizes and normalizes/stems text in documents and sends a token stream to an inverter that aggregates df, tf, posting lists, and hit lists for each term.
The inverted index is stored as 3 separate SQL tables, Lexicon, Posting, and Hit.
An additional forward index table is created from the inverted tables using PostgreSQL.

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
Provide your database credentials in `src/main/resources/config.properties` (example):

```ini
db.url=jdbc:postgresql://database.abcdefg.us-east-1.rds.amazonaws.com:1234/postgresdb
db.user=username
db.pass=password
```

```sh
cd indexer
aws s3 cp s3://<YOUR S3 BUCKET>/out/ out/ --recursive
cat out/part* > merged-index

mvn exec:java merged-index
```