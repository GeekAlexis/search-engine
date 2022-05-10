# PageRank

Hongyu Zhang (hz53@seas.upenn.edu)

This section is divided to 4 parts: Web Link Graph, PageRank, Data, and Eval.

## Web Link Graph

### Source files
- WebLinkGraphDriver.java
- WebLinkGraphMapper.java
- WebLinkGraphReducer.java
- WholeFileInputFormat.java
- WholeFileRecordReader.java

### Features

This part creates the web link graph using MapReduce.     
The mapper queries RDS to get the URL and its outlinks and emits them as intermediate key-value pairs.       
The reducer aggregates the URL, outlinks, and sums up the total out-degree. It also adds a boolean representing whether the URL is in the database. And it adds an initial PageRank value of 1 at the end. Each element in the aggregated output is separated by a unique separator "::??<2<spli75tt," and at the end, a unique record delimiter "::spli75tt." is added.        
The driver sets up the MapReduce job. It reads from the input arguments about the input file path and output file path.

### Install and run
#### Pacakge into JAR
```sh
cd pagerank/weblinkgraph
mvn clean install
```

#### Run Indexer on EMR
EMR version 5.35.0, Hadoop version 2.10.1, Java verion 8.     
Copy weblinkgraph-1.0-SNAPSHOT-launcher.jar from target directory to EC2.       
In EC2 command line, run the following.              

```sh
hadoop jar weblinkgraph-1.0-SNAPSHOT-launcher.jar [input path] [output path]
```


## PageRank

### Source files
- PageRankDriver.java
- PageRankMapper.java
- PageRankReducer.java

### Features

This part builds the iterative PageRank algorithm using MapReduce.     
The mapper computes the average vote each URL can send and writes this average vote to each outlink of this URL. Then it writes the rest of the information to the current URL. To save time and computation, it ignores dangling links (pages that we have not crawled), because these pages do not affect the PageRank values of others.          
The reducer sums up the average votes and apply damping factor of 0.85 and 0.15 to reduce sinks and hogs (self loops). It aggregates the rest of information and the final PageRank value. Similar to the previous part, each element in the aggregated output is separated by a unique separator "::??<2<spli75tt," and at the end, a unique record delimiter "::spli75tt." is added.            
The driver reads from the input arguments about the web link graph output path, output file path, and number of iterations to run. Each iteration builds a MapReduce job for computing PageRank and the output of one iteration is served as input to the next iteration. To save up space, only the last 2 iteration outputs are stored.

### Install and run
#### Pacakge into JAR
```sh
cd pagerank/pagerank
mvn clean install
```

#### Run Indexer on EMR
EMR version 5.35.0, Hadoop version 2.10.1, Java verion 8.     
Copy pagerank-1.0-SNAPSHOT-launcher.jar from target directory to EC2.       
In EC2 command line, run the following.              

```sh
hadoop jar pagerank-1.0-SNAPSHOT-launcher.jar [web link graph path] [output path] [number of iterations]
```


## Data

### Source files
- DataDriver.java
- DataMapper.java
- DataReducer.java

### Features

This part writes the output results of PageRank from S3 to RDS using MapReduce.     
The mapper queries RDS to get the document ID and insert the entry (document ID, PageRank value) to the table PageRank in RDS. It emits nothing.     
The reducer simply does nothing.               
The driver reads from the input arguments about the PageRank output path and output file path. For each input file, it builds a MapReduce job.        
 
### Install and run
#### Pacakge into JAR
```sh
cd pagerank/data
mvn clean install
```

#### Run Indexer on EMR
EMR version 5.35.0, Hadoop version 2.10.1, Java verion 8.     
Copy data-1.0-SNAPSHOT-launcher.jar from target directory to EC2.       
In EC2 command line, run the following.              

```sh
hadoop jar data-1.0-SNAPSHOT-launcher.jar [PageRank path] [output path]
```


## Eval

### Source files
- DifferenceDriver.java
- PrevDifferenceMapper.java
- CurrentDifferenceMapper.java
- DifferenceReducer.java

### Features

This part computes the total PageRank values of last 2 iterations to check for convergence.                
One mapper emits the PageRank value from the previous iteration and one mapper emits the PageRank value from the lastest iteration.                
The reducer simply sums up the values from the previous iteration and the values from the lastest iteration.                             
The driver reads from the input arguments about the PageRank output path of the previous iteration, PageRank output path of the latest iteration, and output file path.        
 
### Install and run
#### Pacakge into JAR
```sh
cd pagerank/eval
mvn clean install
```

#### Run Indexer on EMR
EMR version 5.35.0, Hadoop version 2.10.1, Java verion 8.     
Copy eval-1.0-SNAPSHOT-launcher.jar from target directory to EC2.       
In EC2 command line, run the following.              

```sh
hadoop jar eval-1.0-SNAPSHOT-launcher.jar [PageRank path 1] [PageRank path 2] [output path]
```
