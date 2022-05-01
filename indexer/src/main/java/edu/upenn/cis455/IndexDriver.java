package edu.upenn.cis455;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import edu.upenn.cis455.utils.WholeFileInputFormat;

import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;


public class IndexDriver {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Syntax: IndexDriver {input director} {output directory}");
            System.exit(1);
        }

        setLevel("edu.upenn.cis455", Level.DEBUG);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf);
        job.setJarByClass(IndexDriver.class);
        job.setJobName("Indexer");
        
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputKeyClass(theClass);
        job.setOutputValueClass(theClass);

        job.setMapperClass(Parser.class);
        job.setReducerClass(Inverter.class);
        // job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        try {
            fs.delete(outputDir, true);
        } catch (IOException e) {
            System.err.println("Failed to delete temporary output directory: " + e);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        MultipleOutputs.addNamedOutput(job, "namedOutput", outputFormatClass, keyClass, valueClass);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("Job failed!");
        }
    }
}
