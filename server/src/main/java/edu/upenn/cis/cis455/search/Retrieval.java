package edu.upenn.cis.cis455.search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Queue;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.net.URL;
import java.net.MalformedURLException;
import java.text.Normalizer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.util.StopWatch;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

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


public class Retrieval {
    private static final Logger logger = LogManager.getLogger(Retrieval.class);

    private static final int TERM_CACHE_SIZE = 5000;
    private static final int DOC_CACHE_SIZE = 500000;

    private HikariDataSource ds;

    private static ThreadLocal<TokenizerME> threadLocalTokenizer;
    private static ThreadLocal<SnowballStemmer> threadLocalStemmer;
    private AggregateCharSequenceNormalizer normalizer;
    private Detokenizer detokenizer;

    private Cache<Integer, TermOccurrence> termCache;
    private Cache<Integer, DocumentData> docCache;

    private String vecSql;
    private String docSql;
    private String occSql;
    private String metaSql;

    private double bm25K;
    private double bm25B;
    private double pageRankThresh;
    private double pageRankFactor;
    private int n;
    private double avgDl;

    public Retrieval(double bm25K, double bm25B, double pageRankThresh, double pageRankFactor) {
        this.bm25K = bm25K;
        this.bm25B = bm25B;
        this.pageRankThresh = pageRankThresh;
        this.pageRankFactor = pageRankFactor;

        termCache = CacheBuilder.newBuilder().maximumSize(TERM_CACHE_SIZE).build();
        docCache = CacheBuilder.newBuilder().maximumSize(DOC_CACHE_SIZE).build();

        logger.info("Initializing retrieval...");

        /** Load NLP models */
        try (InputStream modelIn = new URL(TokenizerConfig.TOKENIZER_URL).openStream()) {
            TokenizerModel tokenizerModel = new TokenizerModel(modelIn);
            threadLocalTokenizer = ThreadLocal.withInitial(() -> new TokenizerME(tokenizerModel));
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
        threadLocalStemmer = ThreadLocal.withInitial(
            () -> new SnowballStemmer(SnowballStemmer.ALGORITHM.ENGLISH)
        );

        /** Load config properties file */
        String dbUrl = null;
        String dbUser = null;
        String dbPass = null;
        try (InputStream in = Retrieval.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            prop.load(in);
            dbUrl = prop.getProperty("db.url");
            dbUser = prop.getProperty("db.user");
            dbPass = prop.getProperty("db.pass");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config");
        }

        /** Setup database connection pooling */
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUser);
        config.setPassword(dbPass);
        // Use server-side plan for repeated queries
        config.addDataSourceProperty("prepareThreshold", "1");
        ds = new HikariDataSource(config);

        try (Connection conn = ds.getConnection()) {
            // Precompute corpus size
            String sql = "SELECT COUNT(*) AS n FROM \"DocLength\"";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                ResultSet rs = pstmt.executeQuery();
                rs.next();
                n = rs.getInt("n");
            }
            logger.debug("Precomputed corpus size: {}", n);

            // Precompute average document length
            sql = "SELECT AVG(length) AS avg_dl FROM \"DocLength\"";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                ResultSet rs = pstmt.executeQuery();
                rs.next();
                avgDl = rs.getDouble("avg_dl");
            }
            logger.debug("Precomputed average document length: {}", avgDl);

            vecSql = "SELECT term, id " +
                     "FROM \"Lexicon\" " +
                     "WHERE term = ANY (?)";

            docSql = "SELECT d.doc_id, d.length, r.rank " +
                     "FROM \"DocLength\" d " +
                     "JOIN \"PageRank\" r on r.doc_id = d.doc_id " +
                     "WHERE d.doc_id = ANY (?)";

            occSql = "SELECT l.id, l.df, array_agg(p.doc_id), array_agg(p.tf) " +
                     "FROM \"Lexicon\" l " +
                     "JOIN \"Posting\" p ON p.id >= l.posting_id_offset AND p.id < l.posting_id_offset + l.df " +
                     "WHERE l.id = ANY (?) " +
                     "GROUP BY l.id, l.df";

            metaSql = "SELECT d.id, d.url, d.content, array_agg(distinct(h.position) order by h.position) " +
                      "FROM \"ForwardIndex\" f " +
                      "JOIN \"Hit\" h ON h.id >= f.hit_id_offset AND h.id < f.hit_id_offset + f.n_hit " +
                      "JOIN \"Document\" d ON d.id = f.doc_id " +
                      "WHERE d.id = ANY (?) AND f.term_id = ANY (?) " +
                      "GROUP BY d.id, d.url, d.content";
        }
        catch (SQLException e) {
            logger.error("An error occurred:", e);
            throw new RuntimeException("Failed to initialize retrieval");
        }
    }

   /**
    * The function takes a query string, preprocesses the token then vectorize it by counting
    * the unique terms.
    * 
    * @param query The query string to be vectorized.
    * @return A map of the term ID and their counts.
    */
    public Map<Integer, Integer> vectorize(String query) throws SQLException {
        String[] tokens = threadLocalTokenizer.get().tokenize(query);

        SnowballStemmer stemmer = threadLocalStemmer.get();
        Map<String, Integer> termCounts = new HashMap<>();
        for (String token : tokens) {
            // Normalize
            token = normalizer.normalize(token).toString();
            token = Normalizer.normalize(token, Normalizer.Form.NFKD)
                .replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{Cntrl}]", ""); /* diatrics */
            token = token.replaceAll("[\u00B4\u02B9\u02BC\u02C8\u0301\u2018\u2019\u201B\u2032\u2034\u2037]", "\'"); /* apostrophe */
            token = token.replaceAll("[\u00AB\u00BB\u02BA\u030B\u030E\u201C\u201D\u201E\u201F\u2033\u2036\u3003\u301D\u301E]", "\""); /* quotation mark */
            token = token.replaceAll("[\u00AD\u2010\u2011\u2012\u2013\u2014\u2212\u2015]", "-"); /* hyphen */
            token = token.trim().toLowerCase();

            // Stem
            token = stemmer.stem(token).toString();

            // Remove words that contain whitespace or all punctuations
            if (token.isBlank() || token.matches(".*\\s.*") || token.matches("\\p{Punct}+"))
                continue;

            // Count unique terms
            Integer count = termCounts.get(token);
            if (count == null) {
                count = 0;
            }
            termCounts.put(token, count + 1);
        }
        logger.debug("Key words: {}", termCounts.keySet());

        Map<Integer, Integer> termVec = new HashMap<>();
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement pstmtVec = conn.prepareStatement(vecSql)) {
                pstmtVec.setArray(1, conn.createArrayOf("text", termCounts.keySet().toArray()));
                ResultSet rs = pstmtVec.executeQuery();
                while (rs.next()) {
                    String term = rs.getString(1);
                    int termId = rs.getInt(2);
                    termVec.put(termId, termCounts.get(term));
                }
            }   
        }
        return termVec;
    }

    /**
     * Rank the documents according to query terms and return the topK results.
     * 
     * @param termVec a map of term IDs and their counts in the query
     * @param topk the number of documents to return
     * @return A ordered map of RankScore objects.
     */
    public List<RankScore> rank(Map<Integer, Integer> termVec, int topk) throws SQLException {
        if (termVec.isEmpty()) {
            return null;
        }

        Connection conn = ds.getConnection();
        PreparedStatement pstmtOcc = conn.prepareStatement(occSql);
        PreparedStatement pstmtDoc = conn.prepareStatement(docSql);

        StopWatch watch = new StopWatch("Rank");
        watch.start("Fetch term occurrences");

        Map<Integer, TermOccurrence> occurrences = new HashMap<>();
        Set<Integer> allDocIds = new HashSet<>();

        /** Fetch term occurrences */ 
        Set<Integer> termIdsToFetch = new HashSet<>(termVec.keySet());
        // Fetch from cache if present
        termVec.keySet().forEach(termId -> {
            TermOccurrence occ = termCache.getIfPresent(termId);
            if (occ != null) {
                occurrences.put(termId, occ);
                allDocIds.addAll(occ.getDocIds());
                termIdsToFetch.remove(termId);
            }
        });

        pstmtOcc.setArray(1, conn.createArrayOf("integer", termIdsToFetch.toArray()));
        try (ResultSet rs = pstmtOcc.executeQuery()) {
            while (rs.next()) {
                int termId = rs.getInt(1);
                int df = rs.getInt(2);
                List<Integer> docIds = Arrays.asList((Integer[])rs.getArray(3).getArray());
                List<Integer> tfs = Arrays.asList((Integer[])rs.getArray(4).getArray());

                TermOccurrence occ = new TermOccurrence(df, docIds, tfs);
                occurrences.put(termId, occ);
                termCache.put(termId, occ); // update cache

                allDocIds.addAll(docIds);
            }
        }
        logger.debug("Found {} matches", allDocIds.size());
        if (allDocIds.isEmpty()) {
            return null;
        }
        watch.stop();

        /** Fetch data (pageRank & length) for document matches */ 
        watch.start("Fetch document data");
        Map<Integer, DocumentData> docData = new HashMap<>();
        Set<Integer> docIdsToFetch = new HashSet<>(allDocIds);
        // Fetch from cache if present
        allDocIds.forEach(docId -> {
            DocumentData data = docCache.getIfPresent(docId);
            if (data != null) {
                docData.put(docId, data);
                docIdsToFetch.remove(docId);
            }
        });

        pstmtDoc.setArray(1, conn.createArrayOf("integer", docIdsToFetch.toArray()));
        try (ResultSet rs = pstmtDoc.executeQuery()) {
            while (rs.next()) {
                int docId = rs.getInt(1);
                int dl = rs.getInt(2);
                double pageRank = Math.max(rs.getDouble(3), 0.15);
                DocumentData data = new DocumentData(dl, pageRank);
                docData.put(docId, data);
                docCache.put(docId, data); // update cache
            }
        }
        watch.stop();

        watch.start("BM25 & sort topK");
        /** Compute BM25 for each term in parallel */
        Map<Integer, Double> bm25Vector = new ConcurrentHashMap<>();
        // Initialize BM25 to 0
        docData.forEach((docId, data) -> {
            // Filter documents with low PageRank
            if (data.getPageRank() > pageRankThresh) {
                bm25Vector.put(docId, 0.);
            }
        });

        logger.debug("Ranking {} after filtering", bm25Vector.size());
        occurrences.entrySet().parallelStream().forEach(entry -> {
            int termId = entry.getKey();
            TermOccurrence occurrence = entry.getValue();

            int count = termVec.get(termId);
            int df = occurrence.getDf();
            List<Integer> docIds = occurrence.getDocIds();
            List<Integer> tfs = occurrence.getTfs();

            for (int i = 0; i < docIds.size(); i++) {
                int docId = docIds.get(i);
                if (bm25Vector.containsKey(docId)) {
                    double bm25 = computeBM25(tfs.get(i), df, docData.get(docId).getDl());
                    bm25Vector.put(docId, bm25Vector.get(docId) + bm25 * count);
                }
            }
        });

        /** Combine bm25 and PageRank, rank top K */
        Queue<RankScore> topkHeap = new PriorityQueue<>();
        bm25Vector.forEach((docId, bm25) -> {
            double pageRank = docData.get(docId).getPageRank();
            double weighted = weightScore(bm25, pageRank);

            topkHeap.add(new RankScore(docId, bm25, pageRank, weighted));
            if (topkHeap.size() > topk) {
                topkHeap.poll();
            }
        });

        // Sort top K scores
        List<RankScore> ranks = topkHeap.stream()
            .sorted(Comparator.reverseOrder()).collect(Collectors.toList());       

        pstmtOcc.close();
        pstmtDoc.close();
        conn.close();

        watch.stop();
        logger.debug(watch.prettyPrint());
        return ranks.isEmpty() ? null : ranks;
    }

    /**
     * Retrieve a subset of document urls and metadata given the ranks
     * 
     * @param termVec
     * @param ranks
     * @param offset
     * @param limit
     * @param excerptSize
     * @return
     * @throws SQLException
     */
    public List<RetrievalResult> retrieve(Map<Integer, Integer> termVec,
                                          List<RankScore> ranks,
                                          int offset,
                                          int limit,
                                          int excerptSize) throws SQLException {                
        Connection conn = ds.getConnection();
        PreparedStatement pstmtMeta = conn.prepareStatement(metaSql);

        StopWatch watch = new StopWatch("Retrieve");
        watch.start("Fetch meta data");

        Map<Integer, RetrievalResult> results = new LinkedHashMap<>();
        ranks.stream().skip(offset).limit(limit).forEach(rankScore -> {
            results.put(rankScore.getDocId(),
                        new RetrievalResult(rankScore.getBm25(), rankScore.getPageRank(), rankScore.getScore()));
        });

        pstmtMeta.setArray(1, conn.createArrayOf("integer", results.keySet().toArray()));
        pstmtMeta.setArray(2, conn.createArrayOf("integer", termVec.keySet().toArray()));
        try (ResultSet rs = pstmtMeta.executeQuery()) {
            watch.stop();

            watch.start("Collect results");
            while (rs.next()) {
                int docId = rs.getInt(1);
                RetrievalResult result = results.get(docId);
                try {
                    URL url = new URL(rs.getString(2));
                    String baseUrl = url.getHost();
                    String path = String.join(" › ", url.getPath().split("/")).strip();

                    String content = new String(rs.getBytes(3));
                    Integer[] positions = (Integer[])rs.getArray(4).getArray();

                    Document parsed = Jsoup.parse(content);
                    String title = parsed.title();
                    String excerpt = extractExcerpt(parsed.text(), positions, excerptSize);

                    result.setUrl(url.toString());
                    result.setBaseUrl(baseUrl);
                    result.setPath(path);
                    result.setTitle(title);
                    result.setExcerpt(excerpt);
                } catch (MalformedURLException e) {
                    logger.error("Retrieved URL is invalid");
                }       
            }
        }

        watch.stop();
        logger.debug(watch.prettyPrint());

        pstmtMeta.close();
        conn.close();
        return new ArrayList<>(results.values());
    }

    public void close() {
        if (ds != null) {
            ds.close();
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
     * The weighted score is the sum of the BM25 score and the log of the PageRank score
     *
     * @param bm25 the BM25 score of the document
     * @param pageRank the page rank of the document
     * @return The ranking score.
     */
    private double weightScore(double bm25, double pageRank) {
        return bm25 + pageRankFactor * Math.log(pageRank);
    }

    /**
     * Creates an excerpt and highlights keywords at given positions.
     * Positions must be sorted.
     *
     * @param text The text to be excerpted.
     * @param positions The positions of the tokens in the text that matched the query.
     * @param excerptSize The maximum number of tokens to include in the excerpt.
     * @return A string of the excerpt with the highlighted words.
     */
    private String extractExcerpt(String text, Integer[] positions, int excerptSize) {
        String[] tokens = threadLocalTokenizer.get().tokenize(text);

        int start_pos = Math.max(positions[0] - 5, 0);
        int end_pos = Math.min(start_pos + excerptSize, tokens.length);

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
        setLevel("edu.upenn.cis.cis455", Level.DEBUG);

        Retrieval retrieval = new Retrieval(2.0, 0.75, 0.2, 1.0);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String query = "";
        while (!query.equalsIgnoreCase("exit")) {
            System.out.print("Enter query: ");
            query = reader.readLine();

            StopWatch watch = new StopWatch("Total");
            watch.start("Vectorize");
            Map<Integer, Integer> termVec = retrieval.vectorize(query);
            watch.stop();
            
            watch.start("Rank");
            List<RankScore> ranks = retrieval.rank(termVec, 200);
            watch.stop();
            if (ranks != null) {
                watch.start("Retrieve");
                List<RetrievalResult> data = retrieval.retrieve(termVec, ranks, 0, 10, 50);
                watch.stop();

                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data);
                System.out.println("Results: " + json);
                System.out.println(watch.prettyPrint());
            }
        }

        reader.close();
        retrieval.close();
    }
}
