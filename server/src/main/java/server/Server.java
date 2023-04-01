package server;

import static spark.Spark.*;

import org.apache.logging.log4j.Level;

import server.handlers.NewsHandler;
import server.handlers.SearchHandler;
import server.handlers.YelpHandler;

import static org.apache.logging.log4j.core.config.Configurator.setLevel;

public class Server {
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Syntax: Server {port}");
      System.exit(1);
    }
    setLevel("edu.upenn.cis.cis455", Level.DEBUG);

    port(Integer.parseInt(args[0]));

    // ref: https://gist.github.com/saeidzebardast/e375b7d17be3e0f4dddf
    options("/*", (request, response) -> {
      String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
      if (accessControlRequestHeaders != null) {
        response.header("Access-Control-Allow-Headers",
            accessControlRequestHeaders);
      }

      String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
      if (accessControlRequestMethod != null) {
        response.header("Access-Control-Allow-Methods",
            accessControlRequestMethod);
      }

      return "OK";
    });

    before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));

    get("/search", new SearchHandler());
    get("/news", new NewsHandler());
    get("/yelp", new YelpHandler());

    System.out.println("Server launched!");
  }
}
