package edu.upenn.cis.cis455.handlers;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import static java.util.Map.entry;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.search.Retrieval;
import edu.upenn.cis.cis455.search.RankScore;
import edu.upenn.cis.cis455.search.RetrievalResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SearchHandler implements Route {
	private static final Logger logger = LogManager.getLogger(SearchHandler.class);

	private static final double BM25_K = 2.0;
	private static final double BM25_B = 0.75;
	private static final double PAGERANK_THRESH = 0.2;
	private static final double PAGERANK_FACTOR = 1.0;
	private static final int TOPK = 200;
	private static final int PAGE_SIZE = 10;
	private static final int EXCERPT_SIZE = 30;

	private Retrieval retrieval;
	private ObjectMapper mapper = new ObjectMapper();

	public SearchHandler() {
		retrieval = new Retrieval(BM25_K, BM25_B, PAGERANK_THRESH, PAGERANK_FACTOR);
	}

	@Override
	public Object handle(Request req, Response res) throws HaltException {
		String query = req.queryParams("query");
		String page = req.queryParams("page");
		int pageIdx = (page != null) ? Integer.parseInt(page) - 1 : 0;

		logger.debug("Query: {}", query);

		List<RankScore> ranks = null;
		List<RetrievalResult> data = null;
		try {
			Map<Integer, Integer> termVec = retrieval.vectorize(query);

			ranks = retrieval.rank(termVec, TOPK);
			if (ranks == null) {
				logger.debug("No document match");
				res.status(204);
				return "Your search - " + query + " - did not match any pages";
			}

			int offset = pageIdx * PAGE_SIZE;
			if (offset >= ranks.size()) {
				logger.debug("Page idx out of bound: {}", pageIdx);
				res.status(400);
				return "Requested page out of bound";
			}
			data = retrieval.retrieve(termVec, ranks, offset, PAGE_SIZE, EXCERPT_SIZE);
		} catch (SQLException e) {
			logger.error("Failed to query database");
			res.status(500);
			return e.getMessage();
		}

		res.type("application/json");
		Map<String, Object> results = Map.ofEntries(
			entry("match", ranks.size()),
			entry("data", data)
		);

		String json = null;
		try {
			json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results);
			// Format: {match: n, data: [{url, baseUrl, path, title, excerpt, bm25, pageRank, score}, ...]}
		} catch (JsonProcessingException e) {
			logger.error("Failed to serialize retrieval results");
			res.status(500);
			return e.getMessage();
		}

		return json;
	}
}
