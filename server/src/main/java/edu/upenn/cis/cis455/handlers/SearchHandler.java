package edu.upenn.cis.cis455.handlers;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

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
		retrieval = new Retrieval(dbUrl, 1.2, 0.75, 1.0, 50);
		mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
	}

	@Override
	public Object handle(Request req, Response res) throws HaltException {
		String query = req.queryParams("query");

		List<String> terms = retrieval.preprocess(query);
        List<RetrievalResult> results = retrieval.retrieve(terms, 100);

		String json = null;
		try {
			json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results);
			// {[{url, title, excerpt, bm25, page rank, overall score}, …]}
		} catch (JsonProcessingException e) {
			logger.error("Failed to serialize job");
			res.status(500);
			return e.getMessage();
		}

		return json;
	}
}
