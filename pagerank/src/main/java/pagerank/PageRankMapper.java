package pagerank;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		try {
			String line = value.toString();
			String[] content = line.split(" ");
			int length = content.length;
			StringBuffer info = new StringBuffer(); 
			
			if (length > 2) {
				int outdegree = Integer.parseInt(content[length - 2]);
				double previousRank = Double.parseDouble(content[length - 1]);
				Double averageVote = previousRank / outdegree;
				
				for (int i = 0; i < length - 2; i++) {
					String link = content[i];
					info.append(link + " ");
					context.write(new Text(link), new Text(averageVote.toString()));
				}
				
				info.append(outdegree + " ");
				context.write(new Text(key), new Text(info.toString()));
			} else if (length == 2) {
				info.append(content[0] + " ");
				context.write(new Text(key), new Text(info.toString()));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
