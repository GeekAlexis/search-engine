package edu.upenn.cis455;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.jsoup.Jsoup;
import opennlp.tools.util.normalizer.*;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis455.utils.ParserWritable;


public class Parser extends Mapper<IntWritable, Text, ParserWritable, ParserWritable> {
    private static final Logger logger = LogManager.getLogger(Parser.class);

    // private static final String LANG_DETECTOR_PATH = "langdetect-183.bin";
    // private static final String TOKENIZER_PATH = "opennlp-en-ud-ewt-tokens-1.0-1.9.3.bin";

    private TokenizerME tokenizer;
    private LanguageDetectorME langDetector;
    private SnowballStemmer stemmer;
    private AggregateCharSequenceNormalizer normalizer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.info("Loading NLP models");

        String lang_detector_url = "https://dlcdn.apache.org/opennlp/models/langdetect/1.8.3/langdetect-183.bin";
        String tokenizer_url = "https://dlcdn.apache.org/opennlp/models/ud-models-1.0/opennlp-en-ud-ewt-tokens-1.0-1.9.3.bin";
        
        try (InputStream modelIn = new URL(tokenizer_url).openStream()) {
            tokenizer = new TokenizerME(new TokenizerModel(modelIn));
        }

        try (InputStream modelIn = new URL(lang_detector_url).openStream()) {
            langDetector = new LanguageDetectorME(new LanguageDetectorModel(modelIn));
        }

        normalizer = new AggregateCharSequenceNormalizer(
            new EmojiCharSequenceNormalizer(), 
            new UrlCharSequenceNormalizer(),
            new TwitterCharSequenceNormalizer(),
            new NumberCharSequenceNormalizer(),
            new ShrinkCharSequenceNormalizer()
        );

        stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);
    }

    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        int docId = key.get();
        String docContent = value.toString();

        String docText = Jsoup.parse(docContent).text();
        if (langDetector.predictLanguage(docText).getLang().equals("eng")) {
            String[] tokens = tokenizer.tokenize(docText);
            for (int pos = 0; pos < tokens.length; pos++) {
                String term = processToken(tokens[(int)pos]);
                if (term != null) {
                    logger.debug("Parser emitting term: {}, docId: {}, pos: {}", term, docId, pos);
                    // System.out.println("Parser emitting term: " + term + ", docId: " + docId + ", pos: " + pos);
                    ParserWritable parserOutput = new ParserWritable(term, docId, pos);
                    context.write(parserOutput, parserOutput);
                }
            }
        }
        else {  
            logger.info("Nonenglish document detected, skipping.");
        }
    }

    private String processToken(String token) {
        // Normalize
        token = normalizer.normalize(token).toString();
        // Remove words that contain whitespace or all punctuations
        if (token.matches(".*\\s.*") || token.matches("\\p{Punct}+")) {
            return null;
        }
        // Convert to lowercase and stem
        return stemmer.stem(token.toLowerCase()).toString();
    }

}
