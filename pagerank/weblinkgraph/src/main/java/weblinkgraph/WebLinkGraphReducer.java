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
			
			// Add key url to the output value
			output.append(key.toString() + "::??<2<spli75tt,");
			
			// Add outlinks to value, separated by ::??<2<spli75tt,
			for (Text value:values) {
				String link = value.toString();
				
				if (!link.isEmpty()) {
					output.append(link + "::??<2<spli75tt,");
					outdegree += 1;	
				}
			}
			
			// Add outdegree to value, separated by ::??<2<spli75tt,
			output.append(outdegree + "::??<2<spli75tt,");
			Double initialRank = 1.0;
			// Add initial rank to value, followed by a specified record delimeter ::spli75tt.\n
			output.append(initialRank + "::spli75tt.");
			
			context.write(NullWritable.get(), new Text(output.toString()));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}