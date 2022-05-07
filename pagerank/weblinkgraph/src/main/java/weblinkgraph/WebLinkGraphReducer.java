package weblinkgraph;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

/**
 * Reducer class for WebLinkGraph.
 */
public class WebLinkGraphReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try {
			Integer outdegree = 0;
			StringBuffer output = new StringBuffer();
			Boolean indb = false;
			String separator = "::??<2<spli75tt,";
			String delimiter = "::spli75tt.";
			
			// Add key url to the output value
			output.append(key.toString() + separator);
			
			// Add outlinks to value, separated by ::??<2<spli75tt,
			for (Text value:values) {
				String link = value.toString();
				
				if (link.equals(separator)) {
					indb = true;
				} else if (!link.isEmpty()) {
					output.append(link + separator);
					outdegree += 1;	
				}
			}
			
			// Add outdegree to value, separated by ::??<2<spli75tt,
			output.append(outdegree + separator);
			
			output.append(indb + separator);
			
			Double initialRank = 1.0;
			// Add initial rank to value, followed by a unique record delimiter ::spli75tt.\n
			output.append(initialRank + delimiter);
			
			context.write(NullWritable.get(), new Text(output.toString()));
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}