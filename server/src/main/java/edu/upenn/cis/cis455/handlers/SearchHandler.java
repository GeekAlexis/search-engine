package edu.upenn.cis.cis455.handlers;

import java.sql.SQLException;
import java.util.List;

import spark.Request;
import spark.Route;
import spark.Response;
import spark.HaltException;
import spark.Session;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.cis455.search.Retrieval;
import edu.upenn.cis.cis455.search.RetrievalResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SearchHandler implements Route {
	private static final Logger logger = LogManager.getLogger(SearchHandler.class);

	private Retrieval retrieval;
	private ObjectMapper mapper = new ObjectMapper();

	public SearchHandler(String dbUrl) {
		retrieval = new Retrieval(dbUrl, 1.2, 0.75, 1.0);
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
	}

	@Override
	public Object handle(Request req, Response res) throws HaltException {
		String query = req.queryParams("query");

		List<String> terms = retrieval.preprocess(query);

		List<RetrievalResult> results = null;
		try {
			results = retrieval.retrieve(terms, 100, 50);
		} catch (SQLException e) {
			logger.error("Failed to query database");
			res.status(500);
			return e.getMessage();
		}

		if (results == null) {
			res.status(204);
			return "Your search - " + query + " - did not match any pages";
		}

		res.type("application/json");
		String json = null;
		try {
			json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results);
			logger.debug("Search Results: {}", json);
			// {[{url, title, excerpt, bm25, page rank, overall score}, …]}
		} catch (JsonProcessingException e) {
			logger.error("Failed to serialize retrieval results");
			res.status(500);
			return e.getMessage();
		}

		return json;
	}
}
