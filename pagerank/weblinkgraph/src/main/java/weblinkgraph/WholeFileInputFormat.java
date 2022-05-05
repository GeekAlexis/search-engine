package weblinkgraph;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * Input format for reading the entire file.
 * Implemented by teammate Yukai Yang.
 */
public class WholeFileInputFormat extends CombineFileInputFormat<IntWritable, Text>{
	
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
    
    /**
    * Creates a CombineFileRecordReader to read each file assigned to this InputSplit.
    * Note, that unlike ordinary InputSplits, split must be a CombineFileSplit, and therefore
    * is expected to specify multiple files.
    *
    * @param split The InputSplit to read.  Throws an IllegalArgumentException if this is
    *        not a CombineFileSplit.
    * @param context The context for this task.
    * @return a CombineFileRecordReader to process each file in split.
    *         It will read each file with a WholeFileRecordReader.
    * @throws IOException if there is an error.
    */
    @Override
    public RecordReader<IntWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        if (!(split instanceof CombineFileSplit)) {
            throw new IllegalArgumentException("Split must be a CombineFileSplit");
        }
        return new CombineFileRecordReader<IntWritable, Text>((CombineFileSplit)split, context, WholeFileRecordReader.class);
    }
}

