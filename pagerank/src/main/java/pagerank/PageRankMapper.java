package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.util.Arrays;

/**
 * Mapper class for PageRank.
 */
public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		try {	
			//StringBuffer info = new StringBuffer(); 
			String line = value.toString();
			String[] content = line.split("\\^\\^\\^");
			int length = content.length;
			
			// Get outdegree and rank to compute averaged vote
			int outdegree = Integer.parseInt(content[length - 2]);
			double previousRank = Double.parseDouble(content[length - 1]);
			Double averageVote = previousRank / outdegree;
			
			if (length > 2) {
				for (int i = 0; i < length - 2; i++) {
					String link = content[i];
					//info.append(link + "^^^");
					// Write average vote to each outlink
					context.write(new Text(link), new Text(averageVote.toString()));
				}
			} 
			
			if (length > 0) {
				String info = String.join("^^^", Arrays.copyOf(content, length - 1));
				// Write url's outlink information to the current url
				context.write(new Text(key), new Text(info + "^^^"));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
