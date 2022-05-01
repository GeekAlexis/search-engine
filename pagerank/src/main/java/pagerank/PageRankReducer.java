package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		try {
			StringBuffer output = new StringBuffer();
			Double rank = 0.0;
			
			for (Text value:values) {
				Double vote = null;
				String content = value.toString();
				
				try {
					vote = Double.parseDouble(content);
				} catch (Exception e) {
					output.append(content);
				}
				
				if (vote != null) {
					rank += vote;
				}
			}
			
			rank = 0.85 * rank + 0.15;
			output.append(rank);
			context.write(key, new Text(output.toString()));
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}