package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

/**
 * Reducer class for PageRank.
 */
public class PageRankReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {
			StringBuffer output = new StringBuffer();
			Double rank = 0.0;
			
			for (Text value:values) {
				Double vote = null;
				String content = value.toString();
				
				if (content.contains("::??<2<spli75tt,")) {
					// Get url's outlink information
					output.append(content);
				} else if (!content.isEmpty()) {
					// Get averaged vote
					try {
						vote = Double.parseDouble(content);
						rank += vote;
					} catch (Exception e) {
						e.printStackTrace();
						System.exit(1);
					}
				}
			}
			
			// Add damping factor
			rank = 0.85 * rank + 0.15;
			
			if (!output.toString().isEmpty()) {	
				output.append(rank);
				// Add a unique record delimiter at the end of each record
				context.write(NullWritable.get(), new Text(output.toString() + "::spli75tt."));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}