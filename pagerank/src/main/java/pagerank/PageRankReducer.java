package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

/**
 * Reducer class for PageRank.
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		try {
			StringBuffer output = new StringBuffer();
			Double rank = 0.0;
			
			for (Text value:values) {
				Double vote = null;
				String content = value.toString();
				
				if (content.contains("^^^")) {
					// Get url's outlink information
					output.append(content);
				} else {
					// Get averaged vote
					try {
						vote = Double.parseDouble(content);
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
				}
				
				// sum the averaged vote
				if (vote != null) {
					rank += vote;
				}
			}
			
			// Add damping factor
			rank = 0.85 * rank + 0.15;
			output.append(rank);
			context.write(key, new Text(output.toString()));
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}