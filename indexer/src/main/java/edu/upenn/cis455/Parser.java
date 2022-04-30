package edu.upenn.cis455;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.TokenizerME;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis455.utils.ParserWritable;

public class Parser extends Mapper<LongWritable, Text, Text, ParserWritable> {
    private static final Logger logger = LogManager.getLogger(Parser.class);

    private static final String LANG_DETECTOR_PATH = "langdetect-183.bin";
    private static final String TOKENIZER_PATH = "opennlp-en-ud-ewt-tokens-1.0-1.9.3.bin";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        String lang_detector_url = "https://www.apache.org/dyn/closer.cgi/opennlp/models/langdetect/1.8.3/langdetect-183.bin";
        String tokenizer_url = "https://www.apache.org/dyn/closer.cgi/opennlp/models/ud-models-1.0/opennlp-en-ud-ewt-tokens-1.0-1.9.3.bin";
        
        logger.debug("Downloading NLP models");
        FSDataOutputStream out = fs.create(new Path(LANG_DETECTOR_PATH));
        IOUtils.copyBytes(new URL(lang_detector_url).openStream(), out, conf);

        out = fs.create(new Path(TOKENIZER_PATH));
        IOUtils.copyBytes(new URL(tokenizer_url).openStream(), out, conf);        
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long docId = key.get();
        String docContent = value.toString();

        String docText = Jsoup.parse(docContent).text();

        
    }

}
