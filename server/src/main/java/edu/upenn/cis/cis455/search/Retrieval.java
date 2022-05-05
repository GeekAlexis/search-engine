package edu.upenn.cis.cis455.search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import java.text.Normalizer;
import java.sql.PreparedStatement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.util.StopWatch;

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

    private static final int TERM_CACHE_SIZE = 1000;
    private static final int DOC_CACHE_SIZE = 50000;

    private Connection conn;
    private PreparedStatement pstmtDoc;
    private PreparedStatement pstmtBm25;
    private PreparedStatement pstmtMeta;

    private TokenizerME tokenizer;
    private Detokenizer detokenizer;
    private SnowballStemmer stemmer;
    private AggregateCharSequenceNormalizer normalizer;

    private Map<String, TermOccurrence> termCache;
    private Map<Integer, DocumentData> docCache;
    
    private double bm25K;
    private double bm25B;
    private double pageRankFactor;

    private int n;
    private double avgDl;

    public Retrieval(String dbUrl, double bm25K, double bm25B, double pageRankFactor) {
        this.bm25K = bm25K;
        this.bm25B = bm25B;
        this.pageRankFactor = pageRankFactor;

        termCache = Collections.synchronizedMap(new Cache<>(TERM_CACHE_SIZE));
        docCache = Collections.synchronizedMap(new Cache<>(DOC_CACHE_SIZE));

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
            sql = "SELECT f.doc_id, f.length, r.rank " +
                  "FROM \"ForwardIndex\" f " +
                  "LEFT JOIN \"PageRankMid\" r on r.doc_id = f.doc_id " +
                  "WHERE f.doc_id = ANY (?)";

            pstmtDoc = conn.prepareStatement(sql);

            sql = "SELECT l.term, l.df, array_agg(p.doc_id), array_agg(p.tf)" +
                  "FROM \"Lexicon\" l " +
                  "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                  "WHERE l.term = ANY (?) " +
                  "GROUP BY l.term, l.df";
            pstmtBm25 = conn.prepareStatement(sql);

            // sql = "SELECT url, content " +
            //       "FROM \"Document\" " +
            //       "WHERE id = ANY (?)";
            // pstmtMeta = conn.prepareStatement(sql);

            // sql = "SELECT d.id, d.url, d.content, array_agg(h.position order by h.position) " +
            //       "FROM \"Lexicon\" l " +
            //       "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
            //       "JOIN \"Document\" d ON d.id = p.doc_id " +
            //       "JOIN \"Hit\" h on h.id = p.hit_id_offset " +
            //       "WHERE l.term = ANY (?) AND d.id = ANY (?) " +
            //       "GROUP BY d.id, d.url, d.content";

            sql = "SELECT d.id, d.url, d.content, array_agg(h.position order by h.position) " +
                  "FROM \"Lexicon\" l " +
                  "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                  "JOIN \"Hit\" h on h.id >= p.hit_id_offset AND h.id < p.hit_id_offset + p.tf " +
                  "JOIN \"Document\" d ON d.id = p.doc_id " +
                  "WHERE l.term = ANY (?) AND d.id = ANY (?) " +
                  "GROUP BY d.id, d.url, d.content";
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
            token = Normalizer.normalize(token, Normalizer.Form.NFKD)
                .replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{Cntrl}]", ""); /* diatrics */
            token = token.replaceAll("[\u00B4\u02B9\u02BC\u02C8\u0301\u2018\u2019\u201B\u2032\u2034\u2037]", "\'"); /* apostrophe (') */
            token = token.replaceAll("[\u00AB\u00BB\u02BA\u030B\u030E\u201C\u201D\u201E\u201F\u2033\u2036\u3003\u301D\u301E]", "\""); /* quotation mark (") */
            token = token.replaceAll("[\u00AD\u2010\u2011\u2012\u2013\u2014\u2212\u2015]", "-"); /* hyphen (-) */
            token = token.trim().toLowerCase();

            // Convert to lowercase and stem
            token = stemmer.stem(token).toString();

            // Remove words that contain whitespace or all punctuations
            if (token.isBlank() || token.matches(".*\\s.*") || token.matches("\\p{Punct}+")) {
                continue;
            }
            terms.add(token);
        }
        return terms;
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

        StopWatch watch = new StopWatch();
        watch.start("Fetch term occurrences");

        /** Count unique terms */ 
        Map<String, Integer> termCounts = new HashMap<>();
        for (String term : terms) {
            Integer count = termCounts.get(term);
            if (count == null) {
                count = 0;
            }
            termCounts.put(term, count + 1);
        }        
        
        Map<String, TermOccurrence> occurrences = new HashMap<>();
        Set<Integer> allDocIds = new HashSet<>();

        /** Fetch term occurrences */ 
        Set<String> termsToFetch = new HashSet<>(termCounts.keySet());
        // Fetch from cache if present
        termCounts.keySet().forEach(term -> {
            if (termCache.containsKey(term)) {
                TermOccurrence occ = termCache.get(term);
                occurrences.put(term, occ);
                allDocIds.addAll(occ.getDocIds());
                termsToFetch.remove(term);
            }
        });

        pstmtBm25.setArray(1, conn.createArrayOf("text", termsToFetch.toArray()));
        try (ResultSet rs = pstmtBm25.executeQuery()) {
            while (rs.next()) {
                String term = rs.getString(1);
                int df = rs.getInt(2);
                List<Integer> docIds = Arrays.asList((Integer[])rs.getArray(3).getArray());
                List<Integer> tfs = Arrays.asList((Integer[])rs.getArray(4).getArray());

                allDocIds.addAll(docIds);
                occurrences.put(term, new TermOccurrence(df, docIds, tfs));
            }
        }
        logger.debug("Found {} documents", allDocIds.size());
        termsToFetch.forEach(term -> termCache.put(term, occurrences.get(term))); // update cache
        watch.stop();

        /** Fetch data (pageRank & length) for relevant documents */ 
        watch.start("Fetch document data");
        Map<Integer, DocumentData> docData = new HashMap<>();

        Set<Integer> docIdsToFetch = new HashSet<>(allDocIds);
        // Fetch from cache if present
        allDocIds.forEach(docId -> {
            if (docCache.containsKey(docId)) {
                docData.put(docId, docCache.get(docId));
                docIdsToFetch.remove(docId);
            }
        });

        pstmtDoc.setArray(1, conn.createArrayOf("integer", docIdsToFetch.toArray()));
        try (ResultSet rs = pstmtDoc.executeQuery()) {
            while (rs.next()) {
                int docId = rs.getInt(1);
                int dl = rs.getInt(2);
                double pageRank = Math.max(rs.getDouble(3), 0.15);
                docData.put(docId, new DocumentData(dl, pageRank));
            }
        }
        docIdsToFetch.forEach(docId -> docCache.put(docId, docData.get(docId))); // update cache
        watch.stop();

        watch.start("BM25 and rank topK");
        /** Compute BM25 for each term */    
        Map<Integer, Double> bm25Vector = new HashMap<>();
        allDocIds.forEach(docId -> bm25Vector.put(docId, 0.0));
        for (var entry : occurrences.entrySet()) {
            String term = entry.getKey();
            TermOccurrence occurrence = entry.getValue();

            int count = termCounts.get(term);
            int df = occurrence.getDf();
            List<Integer> docIds = occurrence.getDocIds();
            List<Integer> tfs = occurrence.getTfs();

            for (int i = 0; i < docIds.size(); i++) {
                int docId = docIds.get(i);
                double bm25 = computeBM25(tfs.get(i), df, docData.get(docId).getDl());
                bm25Vector.put(docId, bm25Vector.get(docId) + bm25 * count);
            }
        }
        
        /** Weight bm25 and PageRank in overall score */
        Map<Integer, Double> scoreVector = new HashMap<>();
        bm25Vector.forEach((docId, bm25) -> {
            scoreVector.put(docId, rankingScore(bm25, docData.get(docId).getPageRank()));
        });

        /** Sort and retrieve top K */
        Map<Integer, Double> topkScoreVector = scoreVector.entrySet().parallelStream()
            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
            .limit(topk)
            .collect(Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
                
        watch.stop();

        if (topkScoreVector.isEmpty()) {
            return null;
        }

        watch.start("Fetch doc metadata");
        /** Collect document urls and metadata */ 
        Map<Integer, RetrievalResult> results = new LinkedHashMap<>();
        topkScoreVector.keySet().forEach(docId -> results.put(docId, null));
        
        pstmtMeta.setArray(1, conn.createArrayOf("text", termCounts.keySet().toArray()));
        pstmtMeta.setArray(2, conn.createArrayOf("integer", results.keySet().toArray()));
        try (ResultSet rs = pstmtMeta.executeQuery()) {
            watch.stop();

            watch.start("Construct results");
            while (rs.next()) {
                int docId = rs.getInt(1);
                double bm25 = bm25Vector.get(docId);
                double pageRank = docData.get(docId).getPageRank();
                double score = topkScoreVector.get(docId);

                try {
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
        }

        watch.stop();
        logger.debug(watch.prettyPrint());

        return new ArrayList<>(results.values());
    }

    public void close() {
        if (conn != null) {
            try {
                pstmtDoc.close();
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
        return bm25 + pageRankFactor * Math.log(pageRank);
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

        int start_pos = Math.max(positions[0] - 10, 0);
        int end_pos = Math.min(start_pos + maxTokens, tokens.length);

        for (int pos : positions) {
            tokens[pos] = "<span>" + tokens[pos] + "</span>";
            if (pos >= end_pos) {
                break;
            }
        }

        String[] excerptTokens = Arrays.copyOfRange(tokens, start_pos, end_pos);
        String excerpt = detokenizer.detokenize(excerptTokens, null);
        if (end_pos < tokens.length) {
            excerpt += " ...";
        }
        return excerpt;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Syntax: Retrieval {database url}");
            System.exit(1);
        }
        setLevel("edu.upenn.cis.cis455", Level.DEBUG);

        Retrieval retrieval = new Retrieval(args[0], 1.2, 0.75, 1.0);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String query = "";
        while (!query.equalsIgnoreCase("exit")) {
            System.out.print("Enter query: ");
            query = reader.readLine();

            List<String> terms = retrieval.preprocess(query);
            // List<String> terms = Arrays.asList(query);
            System.out.println("Key words: " + terms);

            List<RetrievalResult> results = retrieval.retrieve(terms, 10, 50);

            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results);

            System.out.println("Results: " + json);
        }

        reader.close();
        retrieval.close();
    }
}
