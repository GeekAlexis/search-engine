package edu.upenn.cis.cis455.search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import com.fasterxml.jackson.databind.ObjectMapper;

import opennlp.tools.util.normalizer.*;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.Detokenizer;
import opennlp.tools.tokenize.DictionaryDetokenizer;
import opennlp.tools.tokenize.DetokenizationDictionary;
import opennlp.tools.stemmer.snowball.SnowballStemmer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;

import edu.upenn.cis.cis455.Config;


public class Retrieval {
    private static final Logger logger = LogManager.getLogger(Retrieval.class);

    private Connection conn;
    private TokenizerME tokenizer;
    private Detokenizer detokenizer;
    private SnowballStemmer stemmer;
    private AggregateCharSequenceNormalizer normalizer;

    private PreparedStatement pstmtOcc;
    private PreparedStatement pstmtBm25;
    private PreparedStatement pstmtMeta;

    private double bm25K;
    private double bm25B;
    private double pageRankFactor;

    private int n;
    private double avgDl;

    public Retrieval(String dbUrl, double bm25K, double bm25B, double pageRankFactor) {
        this.bm25K = bm25K;
        this.bm25B = bm25B;
        this.pageRankFactor = pageRankFactor;

        logger.info("Loading NLP models");
        try (InputStream modelIn = new URL(TokenizerConfig.TOKENIZER_URL).openStream()) {
            tokenizer = new TokenizerME(new TokenizerModel(modelIn));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load tokenizer");
        }

        detokenizer = new DictionaryDetokenizer(new DetokenizationDictionary(
            TokenizerConfig.SPECIAL_TOKENS, TokenizerConfig.DETOKENIZE_RULES
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
            conn = DriverManager.getConnection(dbUrl, Config.DB_USER, Config.DB_PASS);
        } catch (Exception e) {
            logger.error("An error occurred:", e);
            throw new RuntimeException("Failed to open database");
        }
        logger.debug("Opened database successfully");

        // Precompute corpus size from forward index
        String sql = "SELECT COUNT(*) AS n FROM \"ForwardIndex\"";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            n = rs.getInt("n");
        } catch (SQLException e) {
            logger.error("An error occurred:", e);
            throw new RuntimeException("Failed to fetch corpus size");
        }
        logger.debug("Precomputed corpus size: {}", n);

        // Precompute average document length from forward index
        sql = "SELECT AVG(length) AS avg_dl FROM \"ForwardIndex\"";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            avgDl = rs.getDouble("avg_dl");
        } catch (SQLException e) {
            logger.error("An error occurred:", e);
            throw new RuntimeException("Failed to fetch average document length");
        }
        logger.debug("Precomputed average document length: {}", avgDl);

        // Precompile queries
        try {
            //  sql = "SELECT DISTINCT ON (p.doc_id) p.doc_id, f.length, r.rank " +
            //        "FROM \"Lexicon\" l " +
            //        "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
            //        "JOIN \"ForwardIndex\" f on p.doc_id = f.doc_id " +
            //        "JOIN \"PageRank\" r on p.doc_id = r.doc_id " +
            //        "WHERE l.term = ANY (?)";

            sql = "SELECT DISTINCT ON (p.doc_id) p.doc_id, f.length " +
                  "FROM \"Lexicon\" l " +
                  "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                  "JOIN \"ForwardIndex\" f on p.doc_id = f.doc_id " +
                  "WHERE l.term = ANY (?)";
            pstmtOcc = conn.prepareStatement(sql);

            // sql = "SELECT p.doc_id, p.tf, l.df " +
            //       "FROM \"Lexicon\" l " +
            //       "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
            //       "WHERE l.term = ?";

            sql = "SELECT l.term, l.df, array_agg(p.doc_id), array_agg(p.tf)" +
                  "FROM \"Lexicon\" l " +
                  "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                  "WHERE l.term = ANY (?) " +
                  "GROUP BY l.term";
            pstmtBm25 = conn.prepareStatement(sql);

            // sql = "SELECT url, content " +
            //       "FROM \"Document\" " +
            //       "WHERE id = ANY (?)";
            // pstmtMeta = conn.prepareStatement(sql);

            sql = "SELECT d.id, d.url, d.content, array_agg(h.position order by h.position) " +
                  "FROM \"Lexicon\" l " +
                  "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                  "JOIN \"Document\" d ON d.id = p.doc_id " +
                  "JOIN \"Hit\" h on h.id = p.hit_id_offset " +
                  "WHERE l.term = ANY (?) AND d.id = ANY (?) " +
                  "GROUP BY d.id";
            pstmtMeta = conn.prepareStatement(sql);

        } catch (SQLException e) {
            logger.error("An error occurred:", e);
            throw new RuntimeException("Failed to precompile queries");
        }
    }

    /**
     * It takes a query, tokenizes it, normalizes it, removes words that contain whitespace or all
     * punctuations, converts to lowercase and stems it
     *
     * @param query The query string to be preprocessed.
     * @return A list of terms
     */
    public List<String> preprocess(String query) {
        String[] tokens = tokenizer.tokenize(query);
        List<String> terms = new ArrayList<>();

        for (String token : tokens) {
            // Normalize
            token = normalizer.normalize(token).toString();

            // Convert to lowercase and stem
            token = stemmer.stem(token.toLowerCase()).toString();

            // Remove words that contain whitespace or all punctuations
            if (token.isBlank() || token.matches(".*\\s.*") || token.matches("\\p{Punct}+")) {
                continue;
            }
            terms.add(token);
        }
        return terms;
    }

    /**
     * It takes a set of terms and returns a map of document IDs to document occurrences
     *
     * @param terms a set of terms to search for
     * @return A map of docIds to DocOccurrences.
     */
    public Map<Integer, DocOccurrence> findDocOccurrences(Set<String> terms) throws SQLException {
        Map<Integer, DocOccurrence> occurrences = new HashMap<>();

        pstmtOcc.setArray(1, conn.createArrayOf("text", terms.toArray()));
        try (ResultSet rs = pstmtOcc.executeQuery()) {
            while (rs.next()) {
                int docId = rs.getInt(1);
                int docLen = rs.getInt(2);
                // double pageRank = rs.getDouble(3);
                // occurrences.put(docId, new DocOccurrence(docId, docLen, pageRank));

                occurrences.put(docId, new DocOccurrence(docId, docLen, 0));
            }
        }
        return occurrences;
    }

    /**
     * It retrieves the top-k documents that are most relevant to the given query terms
     *
     * @param terms a list of query terms
     * @param topk the number of documents to return
     * @param excerptSize the number of tokens to be included in the excerpt
     * @return A list of RetrievalResult objects or null when there is no match
     */
    public List<RetrievalResult> retrieve(List<String> terms, int topk, int excerptSize) throws SQLException {
        if (terms.isEmpty()) {
            return null;
        }

        // Count unique terms
        Map<String, Integer> termCounts = new HashMap<>();
        for (String term : terms) {
            Integer count = termCounts.get(term);
            if (count == null) {
                count = 0;
            }
            termCounts.put(term, count + 1);
        }

        Map<Integer, DocOccurrence> occurrences = findDocOccurrences(termCounts.keySet());
        logger.debug("Fetched {} doc occurrences: {}", occurrences.size(), occurrences);

        // Compute BM25 for each term
        // Map<Integer, Double> bm25Vector = new HashMap<>();
        // occurrences.keySet().forEach(docId -> bm25Vector.put(docId, 0.0));
        // for (Map.Entry<String, Integer> entry : termCounts.entrySet()) {
        //     String term = entry.getKey();
        //     int count = entry.getValue();

        //     pstmtBm25.setString(1, term);
        //     try (ResultSet rs = pstmtBm25.executeQuery()) {
        //         while (rs.next()) {
        //             int docId = rs.getInt(1);
        //             int tf = rs.getInt(2);
        //             int df = rs.getInt(3);
        //             double bm25 = computeBM25(tf, df, occurrences.get(docId).getDl());
        //             bm25Vector.put(docId, bm25Vector.get(docId) + bm25 * count);
        //         }
        //     }
        // }

        Map<Integer, Double> bm25Vector = new HashMap<>();
        occurrences.keySet().forEach(docId -> bm25Vector.put(docId, 0.0));

        pstmtBm25.setArray(1, conn.createArrayOf("text", termCounts.keySet().toArray()));
        ResultSet rs = pstmtBm25.executeQuery();

        while (rs.next()) {
            String term = rs.getString(1);
            int df = rs.getInt(2);
            Integer[] docIds = (Integer[])rs.getArray(3).getArray();
            Integer[] tfs = (Integer[])rs.getArray(4).getArray();

            int count = termCounts.get(term);
            for (int i = 0; i < docIds.length; i++) {
                double bm25 = computeBM25(tfs[i], df, occurrences.get(docIds[i]).getDl());
                bm25Vector.put(docIds[i], bm25Vector.get(docIds[i]) + bm25 * count);
            }
        }
        rs.close();

        // Weight bm25 and PageRank in overall score
        Map<Integer, Double> scoreVector = new HashMap<>();
        occurrences.forEach((docId, occurrence) -> {
            scoreVector.put(docId, rankingScore(bm25Vector.get(docId), occurrence.getPageRank()));
        });

        // Sort and retrieve top K
        Map<Integer, Double> topkScoreVector = scoreVector.entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .limit(topk)
            .collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        if (topkScoreVector.isEmpty()) {
            return null;
        }

        Map<Integer, RetrievalResult> results = new LinkedHashMap<>();
        topkScoreVector.keySet().forEach(docId -> results.put(docId, null));

        // Collect document urls and metadata
        pstmtMeta.setArray(1, conn.createArrayOf("string", termCounts.keySet().toArray()));
        pstmtMeta.setArray(2, conn.createArrayOf("integer", results.keySet().toArray()));
        rs = pstmtMeta.executeQuery();

        while (rs.next()) {
            try {
                int docId = rs.getInt(1);
                double bm25 = bm25Vector.get(docId);
                double pageRank = occurrences.get(docId).getPageRank();
                double score = topkScoreVector.get(docId);

                URL url = new URL(rs.getString(2));
                String baseUrl = url.getHost();
                String path = String.join(" › ", url.getPath().split("/")).strip();

                String content = new String(rs.getBytes(3));
                Integer[] positions = (Integer[])rs.getArray(4).getArray();

                Document parsed = Jsoup.parse(content);
                String title = parsed.title();
                String excerpt = getExcerpt(parsed.text(), positions, excerptSize);

                results.put(docId, new RetrievalResult(url.toString(), baseUrl, path, title, excerpt, bm25, pageRank, score));
            } catch (MalformedURLException e) {
                logger.error("Retrieved URL is invalid");
            }
        }
        rs.close();

        // List<RetrievalResult> results = new ArrayList<>();
        // for (Map.Entry<Integer, Double> entry : topkScoreVector.entrySet()) {
        //     int docId = entry.getKey();
        //     double score = entry.getValue();
        //     double bm25 = bm25Vector.get(docId);
        //     double pageRank = occurrences.get(docId).getPageRank();

        //     if (!rs.next()) {
        //         throw new SQLException("Retrieved document ID not found");
        //     }

        //     try {
        //         URL url = new URL(rs.getString("url"));
        //         String baseUrl = url.getHost();
        //         String path = String.join(" › ", url.getPath().split("/")).strip();
        //         String content = new String(rs.getBytes("content"));

        //         Document parsed = Jsoup.parse(content);
        //         String title = parsed.title();

        //         String[] tokens = tokenizer.tokenize(parsed.text());
        //         String[] excerptTokens = Arrays.copyOfRange(tokens, 0, excerptSize);
        //         String excerpt = detokenizer.detokenize(excerptTokens, null) + " ...";

        //         results.add(new RetrievalResult(url.toString(), baseUrl, path, title, excerpt, bm25, pageRank, score));
        //     } catch (MalformedURLException e) {
        //         logger.error("Retrieved URL is invalid");
        //     }
        // }
        // rs.close();

        return new ArrayList<>(results.values());
    }

    public void close() {
        if (conn != null) {
            try {
                pstmtOcc.close();
                pstmtBm25.close();
                pstmtMeta.close();
                conn.close();
            } catch (SQLException e) {
                logger.error("Failed to close database:", e);
            }
        }
    }

    /**
     * $$
     * \text{BM25}(q, d) = \sum_{t \in q} \text{idf}(t) \cdot \frac{tf(t, d) \cdot (k_1 + 1)}{tf(t, d)
     * + k_1 \cdot (1 - b + b \cdot \frac{|d|}{avgdl})}
     * $$
     *
     * where (t, d)$ is the term frequency of term $ in document $, $|d|$ is the length of
     * document $, $ is the average document length in the collection, and $ and $ are
     * free parameters
     *
     * @param tf term frequency
     * @param df document frequency
     * @param dl document length
     * @return The BM25 score for the given term.
     */
    private double computeBM25(int tf, int df, int dl) {
        double idf = Math.log((double)n / (double)df);
        double length_norm = (1 - bm25B) + bm25B * (double)dl / avgDl;
        return idf * (bm25K + 1) * (double)tf / (bm25K * length_norm + (double)tf);
    }

    /**
     * The ranking score is the sum of the BM25 score and the log of the PageRank score
     *
     * @param bm25 the BM25 score of the document
     * @param pageRank the page rank of the document
     * @return The ranking score.
     */
    private double rankingScore(double bm25, double pageRank) {
        return bm25 + pageRankFactor * Math.log(pageRank + 1e-5);
    }

    /**
     * Creates an excerpt and highlights keywords at given positions.
     * Positions must be sorted.
     *
     * @param text The text to be excerpted.
     * @param positions The positions of the tokens in the text that matched the query.
     * @param maxTokens The maximum number of tokens to include in the excerpt.
     * @return A string of the excerpt with the highlighted words.
     */
    private String getExcerpt(String text, Integer[] positions, int maxTokens) {
        String[] tokens = tokenizer.tokenize(text);

        int start_pos = positions[0];
        int end_pos = Math.min(tokens.length, start_pos + maxTokens);

        for (int pos : positions) {
            tokens[pos] = "<span>" + tokens[pos] + "</span>";
            if (pos >= end_pos) {
                break;
            }
        }

        String[] excerptTokens = Arrays.copyOfRange(tokens, start_pos, end_pos);
        String excerpt = detokenizer.detokenize(excerptTokens, null) + " ...";
        return excerpt;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Syntax: Retrieval {database url}");
            System.exit(1);
        }
        setLevel("edu.upenn.cis.cis455", Level.DEBUG);

        Retrieval retrieval = new Retrieval(args[0], 1.2, 0.75, 0);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String query = "";
        while (!query.equalsIgnoreCase("exit")) {
            System.out.print("Enter query: ");
            query = reader.readLine();

            List<String> terms = retrieval.preprocess(query);
            // List<String> terms = Arrays.asList(query.split("\\s"));
            // List<String> terms = Arrays.asList(query);
            System.out.println("Key words: " + terms);
            List<RetrievalResult> results = retrieval.retrieve(terms, 100, 50);

            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results);

            System.out.println("Results: " + json);
        }

        reader.close();
        retrieval.close();
    }
}
