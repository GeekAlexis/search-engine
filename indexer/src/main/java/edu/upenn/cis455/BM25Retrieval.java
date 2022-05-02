package edu.upenn.cis455;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import opennlp.tools.util.normalizer.*;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;


public class BM25Retrieval {
    private static final Logger logger = LogManager.getLogger(BM25Retrieval.class);

    private static final String USER = "postgres";
    private static final String PASS = "cis555db";

    private Connection conn;
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
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(dbUri, USER, PASS);
        } catch (Exception e) {
            logger.error("Failed to open database:", e);
        }

        logger.debug("Opened database successfully");
    }

    public List<String> preprocess(String query) throws Exception {
        String[] tokens = tokenizer.tokenize(query);
        List<String> terms = new ArrayList<>();

        for (String token : tokens) {
            // Normalize
            for (var normalizer : normalizers) {
                token = normalizer.normalize(token).toString();
            }
            // Remove words that contain whitespace or all punctuations
            if (token.matches(".*\\s.*") || token.matches("\\p{Punct}+")) {
                continue;
            }
            // Convert to lowercase and stem
            token = stemmer.stem(token.toLowerCase()).toString();
            terms.add(token);
        }
        return terms;
    }

    /**
     * 
     * @param terms
     * @return
     */
    public Set<Integer> getOccurrenceDocIds(List<String> terms) {
        // String termSlots = Collections.nCopies(terms.size(), "?").stream()
        //     .collect(Collectors.joining(", ", "(", ")"));
        // String sql = "SELECT DISTINCT ON (d.id), f.length" +
        //              "FROM \"Lexicon\" l, \"Posting\" p, \"ForwardIndex\" f" +
        //              "WHERE p.id BETWEEN (l.posting_id_offset, l.df) AND f.doc_id = d.id AND l.term IN " + termSlots;

        // try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
        //     for (int i = 0; i < terms.size(); i++) {
        //         pstmt.setString(i, terms.get(i));
        //     }
            
        //     ResultSet rs = pstmt.executeQuery();
        //     while (rs.next()) {
                
        //     }
        // } catch (SQLException e) {
        //     logger.error("An error occurred:", e);
        // }
        return null;
    }
  
    public Map<Integer, Double> retrieveDocs(List<String> terms, int topk) {
        Map<String, Integer> counts = new HashMap<>();
        for (String term : terms) {
            Integer count = counts.get(term);
            if (count == null) {
                count = 0;
            }
            counts.put(term, count + 1);
        }

        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            String term = entry.getKey();
            int count = entry.getValue();


        }

        Map<Integer, Double> results = new HashMap<>();
        
        return null;
    }

    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Failed to close database:", e);
            }
        }
    }

    public static void main(String[] args) throws IOException, Exception {
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
            System.out.println("Key words: " + terms);
            Map<Integer, Double> results = bm25Retrieval.retrieveDocs(terms, 100);
    
            System.out.println("Results: " + results);    
        }

        reader.close();
        bm25Retrieval.close();
       
    }
}
