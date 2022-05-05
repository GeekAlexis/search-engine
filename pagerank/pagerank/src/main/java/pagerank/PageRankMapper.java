package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper class for PageRank.
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			String line = value.toString();
			// Split value based on the specified separator
			String[] content = line.split("::\\?\\?<2<spli75tt,");
			int length = content.length;
			
			// Get url, outdegree, and previous rank
			String url = content[0];
			int outdegree = Integer.parseInt(content[length - 2]);
			double previousRank = Double.parseDouble(content[length - 1]);
			Double averageVote;
			
			// Compute average vote
			if (outdegree != 0) { 
				averageVote = previousRank / outdegree;
				
				if (length > 3) {
					for (int i = 1; i < length - 2; i++) {
						String link = content[i];
						// Write average vote to each outlink
						context.write(new Text(link), new Text(averageVote.toString()));
					}
				}
			}
				
			String info = String.join("::??<2<spli75tt,", Arrays.copyOf(content, length - 1));
			// Write url's outlink information to the current url
			context.write(new Text(url), new Text(info + "::??<2<spli75tt,"));

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
