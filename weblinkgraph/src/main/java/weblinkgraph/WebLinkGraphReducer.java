package weblinkgraph;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

/**
 * Reducer class for WebLinkGraph.
 */
public class WebLinkGraphReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try {
			Integer outdegree = 0;
			StringBuffer output = new StringBuffer();
			
			// add outlinks to value, separated by ^^^
			for (Text value:values) {
				String link = value.toString();
				
				if (!link.isEmpty()) {
					output.append(link + "^^^");
					outdegree += 1;	
				}
			}
			
			// add outdegree to value, separated by ^^^
			output.append(outdegree + "^^^");
			Double initialRank = 1.0;
			// add initial rank to value
			output.append(initialRank);
			
			context.write(key, new Text(output.toString()));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}