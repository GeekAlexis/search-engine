package data;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {
			context.write(NullWritable.get(), new Text("done"));	
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
}