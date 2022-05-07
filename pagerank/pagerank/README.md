# PageRank

Hongyu Zhang hz53@seas.upenn.edu

This is the PageRank section of the CIS 555 Final Project. It has 4 components: PageRankDriver, PageRankMapper, PageRankReducer, and DataTransfer.

The first component PageRankDriver is the driver program for map-reduce. It reads input from S3 and output the final result to S3. For each iteration of PageRank, it sets up a map-reduce job. And in each iteration, the job takes the output of the previous iteration as the input.

The second component PageRankMapper is the mapper for the map-reduce process. In each mapper, it computes the average vote that the url can send. It writes the average vote to each outlink of this url as key and value. It also writes the url's outlink information to the current url.

The third component PageRankReducer is the reducer for the map-reduce process. In each reducer, it sums up the average votes the url receives. It uses damping factor of 0.85 and 0.15 in the process to reduce sinks and hogs. It writes the outlink information of this url and its rank to the url as key and value. 

The last component DataTransfer reads the output data of final map-reduce from S3 and creates a table in RDS to store the rank values.

