package edu.upenn.cis.cis455;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.Normalizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.jsoup.Jsoup;

import edu.upenn.cis.cis455.utils.ParserWritable;
import opennlp.tools.util.normalizer.*;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Parser extends Mapper<IntWritable, Text, ParserWritable, ParserWritable> {
    private static final Logger logger = LogManager.getLogger(Parser.class);

    private static final int MAX_TERM_LEN = 100;

    private TokenizerME tokenizer;
    private LanguageDetectorME langDetector;
    private SnowballStemmer stemmer;
    private AggregateCharSequenceNormalizer normalizer;

    private Matcher punctMatcher;
    private Matcher whiteSpaceMatcher;

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

        Pattern punctPattern = Pattern.compile("\\p{Punct}+", Pattern.UNICODE_CHARACTER_CLASS);
        Pattern whiteSpacePattern = Pattern.compile(".*\\s.*", Pattern.UNICODE_CHARACTER_CLASS);
        punctMatcher = punctPattern.matcher("");
        whiteSpaceMatcher = whiteSpacePattern.matcher("");
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
        token = Normalizer.normalize(token, Normalizer.Form.NFKD)
            .replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{Cntrl}]", ""); /* diatrics */
        token = token.replaceAll("[\u00B4\u02B9\u02BC\u02C8\u0301\u2018\u2019\u201B\u2032\u2034\u2037]", "\'"); /* apostrophe (') */
        token = token.replaceAll("[\u00AB\u00BB\u02BA\u030B\u030E\u201C\u201D\u201E\u201F\u2033\u2036\u3003\u301D\u301E]", "\""); /* quotation mark (") */
        token = token.replaceAll("[\u00AD\u2010\u2011\u2012\u2013\u2014\u2212\u2015]", "-"); /* hyphen (-) */
        token = token.trim().toLowerCase();

        // Convert to lowercase and stem
        token = stemmer.stem(token).toString();

        // Remove terms that contain whitespace or all punctuations
        punctMatcher.reset(token);
        whiteSpaceMatcher.reset(token);
        if (token.isBlank() || whiteSpaceMatcher.matches()
            || punctMatcher.matches() || token.length() > MAX_TERM_LEN) {
            return null;
        }
        return token;
    }

}
