package convergence;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

/**
 * Mapper class for PageRank.
 */
public class CurrentDifferenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		try {
			String line = value.toString();
			// Split value based on the specified separator
			String[] content = line.split("::\\?\\?<2<spli75tt,");
			int length = content.length;
			
			// Get current rank
			double rank = Double.parseDouble(content[length - 1]);
			context.write(new Text("current"), new DoubleWritable(rank));
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}
