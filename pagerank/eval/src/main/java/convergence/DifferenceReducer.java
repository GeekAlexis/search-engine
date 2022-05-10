package convergence;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.io.IOException;

/**
 * Reducer class for PageRank.
 */
public class DifferenceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		try {
			Double difference = 0.0;
			
			// Sum up the values
			for (DoubleWritable value:values) {
				double rank = value.get();
				difference += rank;
			}
			
			context.write(key, new DoubleWritable(difference));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}