package edu.upenn.cis455;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import opennlp.tools.util.normalizer.*;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis455.storage.StorageFactory;
import edu.upenn.cis455.storage.StorageSQL;

import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;


public class BM25Retrieval {
    private static final Logger logger = LogManager.getLogger(BM25Retrieval.class);

    private StorageSQL db;
    private TokenizerME tokenizer;
    private SnowballStemmer stemmer;
    private CharSequenceNormalizer[] normalizers;

    public BM25Retrieval(String dbUri) {
        logger.info("Loading NLP models");

        String tokenizer_url = "https://dlcdn.apache.org/opennlp/models/ud-models-1.0/opennlp-en-ud-ewt-tokens-1.0-1.9.3.bin";
        
        try (InputStream modelIn = new URL(tokenizer_url).openStream()) {
            tokenizer = new TokenizerME(new TokenizerModel(modelIn));
        } catch (IOException e) {
            logger.error("Failed to load tokenizer");
        }

        normalizers = new CharSequenceNormalizer[] {
            new EmojiCharSequenceNormalizer(), 
            new UrlCharSequenceNormalizer(),
            new TwitterCharSequenceNormalizer(),
            new NumberCharSequenceNormalizer(),
            new ShrinkCharSequenceNormalizer()
        };

        stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);

        try {
            db = (StorageSQL)StorageFactory.getDatabaseInstance(dbUri);
        } catch (Exception e) {
            logger.error("Failed to open database:", e);
        }

        logger.debug("Opened database successfully");
    }

    public List<String> preprocess(String query) {
        String[] tokens = tokenizer.tokenize(query);
        List<String> terms = new ArrayList<>();

        for (String token : tokens) {
            // Normalize
            for (var normalizer : normalizers) {
                token = normalizer.normalize(token).toString();
            }
            // Remove whitespace or single punctuation
            if (token.matches("\\s+") || token.matches("\\p{Punct}")) {
                token = "";
            }
            // Convert to lowercase and stem
            token = stemmer.stem(token.toLowerCase()).toString();

            if (!token.isEmpty()) {
                terms.add(token);
            }
        }
        return terms;
    }

    public Map<Integer, Double> retrieveDocs(List<String> terms, int topk) {
        Map<Integer, Double> results = new HashMap<>();
        
        return null;
    }

    public void close() {
        if (db != null) {
            try {
                db.close();
            } catch (SQLException e) {
                logger.error("Failed to close database:", e);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Syntax: QueryRunner {database url}");
            System.exit(1);
        }
        setLevel("edu.upenn.cis455", Level.DEBUG);

        BM25Retrieval bm25Retrieval = new BM25Retrieval(args[0]);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        String query = "";
        while (!query.equalsIgnoreCase("exit")) {
            System.out.print("Enter query: ");
            query = reader.readLine();
    
            List<String> terms = bm25Retrieval.preprocess(query);
            System.out.println(terms);
            Map<Integer, Double> results = bm25Retrieval.retrieveDocs(terms, 100);
    
            System.out.println("Results: " + results);    
        }

        reader.close();
        bm25Retrieval.close();
       
    }
}
