package edu.upenn.cis455;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.upenn.cis455.utils.WholeFileInputFormat;
import edu.upenn.cis455.utils.ParserPartitioner;
import edu.upenn.cis455.utils.ParserWritable;
import edu.upenn.cis455.utils.ParserGroupingComparator;

import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;


public class IndexDriver {
    public static void main(String[] args) throws Exception {
        // args: s3://555docbucket/in/ s3://indexer-mapreduce/out/ temp/
        if (args.length != 3) {
            System.err.println("Syntax: IndexDriver {input director} {output directory} {storage directory}");
            System.exit(1);
        }

        setLevel("edu.upenn.cis455", Level.DEBUG);

        Configuration conf = new Configuration();
        conf.set("storageDir", args[2]);
        conf.set("mapreduce.input.fileinputformat.split.maxsize","268435456");
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        FileSystem fs = FileSystem.get(new URI(args[1]), conf);

        Job job = Job.getInstance(conf);
        job.setJarByClass(IndexDriver.class);
        job.setJobName("Indexer");

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setMapOutputKeyClass(ParserWritable.class);
        job.setMapOutputValueClass(ParserWritable.class);  
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Parser.class);
        job.setReducerClass(Inverter.class);
        job.setPartitionerClass(ParserPartitioner.class);
        job.setGroupingComparatorClass(ParserGroupingComparator.class);
        // job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputDir = new Path(args[1]);
        try {
            fs.delete(outputDir, true);
        } catch (IOException e) {
            System.err.println("Failed to delete temporary output directory: " + e);
        }
        FileOutputFormat.setOutputPath(job, outputDir);

        System.out.println("Starting indexer job...");

        if (!job.waitForCompletion(true)) {
            System.err.println("Job failed!");
            System.exit(1);
        }

        System.out.println("Job completed!");
    }
}
