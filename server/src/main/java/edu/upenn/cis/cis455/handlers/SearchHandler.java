package edu.upenn.cis.cis455.handlers;

import spark.Request;
import spark.Route;
import spark.Response;

import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import spark.HaltException;
import spark.Session;

import edu.upenn.cis.cis455.search.Retrieval;
import edu.upenn.cis.cis455.search.RetrievalResult;

public class SearchHandler implements Route {
	private Retrieval retrieval;

	public SearchHandler(String dbUri) {
		retrieval = Retrieval(dbUri, 1.2, 0.75, 1.0, 50);
	}

	@Override
	public Object handle(Request req, Response res) throws HaltException {
		String query = req.queryParams("query");

		List<String> terms = retrieval.preprocessQuery(query);
        List<RetrievalResult> results = retrieval.retrieve(terms, 100);

		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(results);
		// {[{url, title, excerpt, bm25, page rank, overall score}, …]}

		return json;
	}
}
