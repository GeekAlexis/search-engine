package edu.upenn.cis.cis455;

import static spark.Spark.*;

import main.java.edu.upenn.cis.cis455.handlers.SearchHandler;
import main.java.edu.upenn.cis.cis455.handlers.NewsHandler;
import main.java.edu.upenn.cis.cis455.handlers.YelpHandler;

public class Server {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Syntax: Server {port} {database url} ");
			System.exit(1);
		}

		port(args[0]);

		get("/hello", (req, res) -> "Hello World");
		get("/search", new SearchHandler(args[1]));
		get("/news", new NewsHandler());
		get("/yelp/:term/:location", new YelpHandler());
	}
}
