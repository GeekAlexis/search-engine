package edu.upenn.cis.cis455;

import static spark.Spark.*;

import edu.upenn.cis.cis455.handlers.SearchHandler;
import edu.upenn.cis.cis455.handlers.NewsHandler;
import edu.upenn.cis.cis455.handlers.YelpHandler;

import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;

public class Server {
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Syntax: Server {port} {database url} ");
			System.exit(1);
		}
		setLevel("edu.upenn.cis.cis455", Level.DEBUG);

		port(Integer.parseInt(args[0]));

		get("/search", new SearchHandler(args[1]));
		get("/news", new NewsHandler());
		get("/yelp/:term/:location", new YelpHandler());

		System.out.println("Server launched!");
	}
}
