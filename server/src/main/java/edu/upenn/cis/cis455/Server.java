package edu.upenn.cis.cis455;

import static spark.Spark.*;

import edu.upenn.cis.cis455.handlers.SearchHandler;
import edu.upenn.cis.cis455.handlers.NewsHandler;
import edu.upenn.cis.cis455.handlers.YelpHandler;

public class Server {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Syntax: Server {port} {database url} ");
			System.exit(1);
		}

		port(Integer.parseInt(args[0]));

		get("/search", new SearchHandler(args[1]));
		get("/news", new NewsHandler());
		get("/yelp/:term/:location", new YelpHandler());
	}
}
