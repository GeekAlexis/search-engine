# Web Link Graph

Hongyu Zhang hz53@seas.upenn.edu

This section creates the web link graph from the crawler and it is used in PageRank. It has 4 components.

The first component WebLinkGraphDriver is the driver program for map-reduce. It reads input from S3, sets a job for map-reduce and output the final result to S3. For each iteration o, it sets up a map-reduce job.

The second component WebLinkGraphMapper is the mapper for the map-reduce process. In mapper, it gets the url's outlinks from RDS, and for each outlink, it writes <url, outlink> as intermediate pairs.

The third component WebLinkGraphReducer is the reducer for the map-reduce process. In reducer, it sums up the number of outlinks that the url has. In the end, the output key is url, and the output value is a Text that includes each outlink of this url, outdegree of this url, and an initial rank value 1, separated by "^^^". 

The last component is the WholeFileInputFormat and WholeFileRecordReader implemented by my teammate Yukai Yang. They are used for reading the entire file in the mapping stage.