package edu.upenn.cis455;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import opennlp.tools.util.normalizer.*;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.Detokenizer;
import opennlp.tools.tokenize.DictionaryDetokenizer;
import opennlp.tools.tokenize.DetokenizationDictionary;
import opennlp.tools.tokenize.DetokenizationDictionary.Operation;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;


public class Retrieval {
    private static final Logger logger = LogManager.getLogger(Retrieval.class);

    private static final String TOKENIZER_URL = "https://dlcdn.apache.org/opennlp/models/ud-models-1.0/opennlp-en-ud-ewt-tokens-1.0-1.9.3.bin";
    private static final String USER = "postgres";
    private static final String PASS = "cis555db";

    private static final String[] SPECIAL_TOKENS = {".", "?", "!", ":", ";", "\"", "'"};
    private static final Operation[] DETOKENIZE_RULES = {
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.MOVE_LEFT,
        Operation.RIGHT_LEFT_MATCHING,
        Operation.RIGHT_LEFT_MATCHING
    };

    private Connection conn;
    private TokenizerME tokenizer;
    private Detokenizer detokenizer;
    private SnowballStemmer stemmer;
    private AggregateCharSequenceNormalizer normalizer;

    private double bm25K;
    private double bm25B;
    private double pageRankFactor;
    private int excerptSize;
    
    private int n;
    private double avgDl;

    public Retrieval(String dbUri, double bm25K, double bm25B, double pageRankFactor, int excerptSize) {
        this.bm25K = bm25K;
        this.bm25B = bm25B;
        this.pageRankFactor = pageRankFactor;
        this.excerptSize = excerptSize;

        logger.info("Loading NLP models");
        try (InputStream modelIn = new URL(TOKENIZER_URL).openStream()) {
            tokenizer = new TokenizerME(new TokenizerModel(modelIn));
        } catch (IOException e) {
            logger.error("Failed to load tokenizer");
        }

        detokenizer = new DictionaryDetokenizer(new DetokenizationDictionary(
            SPECIAL_TOKENS, DETOKENIZE_RULES
        ));
        normalizer = new AggregateCharSequenceNormalizer(
            new EmojiCharSequenceNormalizer(), 
            new UrlCharSequenceNormalizer(),
            new TwitterCharSequenceNormalizer(),
            new NumberCharSequenceNormalizer(),
            new ShrinkCharSequenceNormalizer()
        );
        stemmer = new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH);

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(dbUri, USER, PASS);
        } catch (Exception e) {
            logger.error("Failed to open database:", e);
        }
        logger.debug("Opened database successfully");

        // Precompute corpus size and average document length from forward index
        String sql = "SELECT COUNT(*) AS n FROM \"ForwardIndex\"";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            n = rs.getInt("n");
        } catch (SQLException e) {
            logger.error("An error occurred:", e);
        }

        sql = "SELECT AVG(length) AS avg_dl FROM \"ForwardIndex\"";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            avgDl = rs.getDouble("avg_dl");
        } catch (SQLException e) {
            logger.error("An error occurred:", e);
        }
    }

    public List<String> preprocessQuery(String query) throws Exception {
        String[] tokens = tokenizer.tokenize(query);
        List<String> terms = new ArrayList<>();

        for (String token : tokens) {
            // Normalize
            token = normalizer.normalize(token).toString();
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
    public Map<Integer, DocOccurrence> getDocOccurrences(Set<String> terms) {
        String termSlots = Collections.nCopies(terms.size(), "?").stream()
            .collect(Collectors.joining(", ", "(", ")"));

        // String sql = "SELECT DISTINCT ON (p.doc_id) p.doc_id, f.length, r.page_rank " +
        //              "FROM \"Lexicon\" l, \"Posting\" p, \"ForwardIndex\" f, \"PageRank\" r " +
        //              "WHERE p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
        //              "AND f.doc_id = p.doc_id AND r.doc_id = p.doc_id AND l.term IN " + termSlots;

        String sql = "SELECT DISTINCT ON (p.doc_id) p.doc_id, f.length " +
                     "FROM \"Lexicon\" l, \"Posting\" p, \"ForwardIndex\" f " +
                     "WHERE p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                     "AND f.doc_id = p.doc_id AND l.term IN " + termSlots;

        logger.debug(sql);

        Map<Integer, DocOccurrence> occurrences = new HashMap<>();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int i = 1;
            for (String term : terms) {
                pstmt.setString(i++, term);
            }
            
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int docId = rs.getInt(1);
                int docLen = rs.getInt(2);
                // double pageRank = rs.getDouble(3);
                // occurrences.put(docId, new DocOccurrence(docId, docLen, pageRank));

                occurrences.put(docId, new DocOccurrence(docId, docLen, 0));
            }
        } catch (SQLException e) {
            logger.error("An error occurred when fetching occurrences:", e);
        }
        return occurrences;
    }

    /**
     * 
     * @param terms
     * @param topk
     * @return
     */
    public List<RetrievalResult> retrieve(List<String> terms, int topk) {
        if (terms.size() == 0) {
            return null;
        }

        // Count unique terms
        Map<String, Integer> counts = new HashMap<>();
        for (String term : terms) {
            Integer count = counts.get(term);
            if (count == null) {
                count = 0;
            }
            counts.put(term, count + 1);
        }

        Map<Integer, DocOccurrence> occurrences = getDocOccurrences(counts.keySet());
        Map<Integer, Double> bm25Vector = new HashMap<>();
        occurrences.forEach((docId, occurence) -> bm25Vector.put(docId, 0.0));
        logger.debug(occurrences);

        // Compute BM25
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            String term = entry.getKey();
            int count = entry.getValue();

            String sql = "SELECT p.doc_id, p.tf, l.df " +
                         "FROM \"Lexicon\" l, \"Posting\" p " +
                         "WHERE p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                         "AND l.term = ?";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {        
                pstmt.setString(1, term);        
                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    int docId = rs.getInt(1);
                    int tf = rs.getInt(2);
                    int df = rs.getInt(3);
                    double bm25 = computeBM25(tf, df, occurrences.get(docId).getDl());
                    bm25Vector.put(docId, bm25Vector.get(docId) + bm25 * count);
                }
            } catch (SQLException e) {
                logger.error("An error occurred when computing BM25:", e);
            }
        }

        Map<Integer, Double> scoreVector = new HashMap<>();
        occurrences.forEach((docId, occurence) -> {
            scoreVector.put(docId, computeScore(bm25Vector.get(docId), occurence.getPageRank()));
        });

        // Sort and retrieve top K
        Map<Integer, Double> topkScoreVector = scoreVector.entrySet().parallelStream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .limit(topk)
            .collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        if (topkScoreVector.size() == 0) {
            return null;
        }

        // Collect document urls and metadata
        String docIdSlots = Collections.nCopies(topkScoreVector.size(), "?").stream()
                .collect(Collectors.joining(", ", "(", ")"));
        String sql = "SELECT url, content" +
                     "FROM \"Document\" " +
                     "WHERE id IN " + docIdSlots;
        logger.debug(sql);

        ResultSet rs = null;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {        
            int i = 1;
            for (int docId : topkScoreVector.keySet()) {
                pstmt.setInt(i++, docId);
            }

            rs = pstmt.executeQuery();
        } catch (SQLException e) {
            logger.error("An error occurred when collecting documents:", e);
        }
        
        List<RetrievalResult> results = new ArrayList<>();
        for (Map.Entry<Integer, Double> entry : topkScoreVector.entrySet()) {
            int docId = entry.getKey();
            double score = entry.getValue();
            double bm25 = bm25Vector.get(docId);
            double pageRank = occurrences.get(docId).getPageRank();

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {        
                rs.next();        
                
                String url = rs.getString("url");
                String content = new String(rs.getBytes("content"));

                Document parsed = Jsoup.parse(content);
                String title = parsed.title();
                String docText = parsed.text();

                String[] tokens = tokenizer.tokenize(docText);
                String[] excerptTokens = Arrays.copyOfRange(tokens, 0, excerptSize);
                String excerpt = detokenizer.detokenize(excerptTokens, null) + " ...";

                results.add(new RetrievalResult(url, title, excerpt, bm25, pageRank, score));

            } catch (SQLException e) {
                logger.error("An error occurred when adding results:", e);
            }
        }
        // {“data”:  [{url, title, excerpt, tf-idf (for each term/query), page rank, overall score}, …]}
        
        return results;
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

    private double computeBM25(int tf, int df, int dl) {
        double idf = Math.log((double)n / (double)df);
        double length_norm = (1 - bm25B) + bm25B * (double)dl / avgDl;
        return idf * (bm25K + 1) * (double)tf / (bm25K * length_norm + (double)tf);
    }

    private double computeScore(double bm25, double pageRank) {
        return bm25 + pageRankFactor * Math.log(pageRank);
    }

    public static void main(String[] args) throws IOException, Exception {
        if (args.length < 1) {
            System.err.println("Syntax: Retrieval {database url}");
            System.exit(1);
        }
        setLevel("edu.upenn.cis455", Level.DEBUG);

        Retrieval retrieval = new Retrieval(args[0], 1.2, 0.75, 1.0, 50);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        String query = "";
        while (!query.equalsIgnoreCase("exit")) {
            System.out.print("Enter query: ");
            query = reader.readLine();
    
            List<String> terms = retrieval.preprocessQuery(query);
            System.out.println("Key words: " + terms);
            List<RetrievalResult> results = retrieval.retrieve(terms, 100);
    
            System.out.println("Results: " + results);    
        }

        reader.close();
        retrieval.close();
       
    }
}
