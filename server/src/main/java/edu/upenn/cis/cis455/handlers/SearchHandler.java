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

	private static final int PAGE_SIZE = 10;
	private static final int EXCERPT_SIZE = 50;
	private static final int TOPK = 100;

	private Retrieval retrieval;
	private ObjectMapper mapper = new ObjectMapper();

	public SearchHandler() {
		retrieval = new Retrieval(1.2, 0.75, 1.0);
	}

	@Override
	public Object handle(Request req, Response res) throws HaltException {
		String query = req.queryParams("query");
		int page = Integer.parseInt(req.queryParams("page"));

		Map<Integer, RankScore> ranks = null;
		List<RetrievalResult> data = null;
		try {
			Map<String, Integer> termCounts = retrieval.vectorize(query);
			ranks = retrieval.rank(termCounts, TOPK);

			if (ranks == null) {
				res.status(204);
				return "Your search - " + query + " - did not match any pages";
			}
			int offset = (page - 1) * PAGE_SIZE;
			if (offset >= ranks.size()) {
				res.status(400);
				return "Page " + page + " is out of bound";
			}
			data = retrieval.retrieve(ranks, termCounts.keySet(), offset, PAGE_SIZE, EXCERPT_SIZE);
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
			logger.debug("Search Results: {}", json);
			// Format: {match: n, data: [{url, baseUrl, path, title, excerpt, bm25, pageRank, score}, ...]}
		} catch (JsonProcessingException e) {
			logger.error("Failed to serialize retrieval results");
			res.status(500);
			return e.getMessage();
		}

		return json;
	}
}
