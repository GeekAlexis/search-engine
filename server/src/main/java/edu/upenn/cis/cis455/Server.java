package edu.upenn.cis.cis455;

import static spark.Spark.*;

import main.java.edu.upenn.cis.cis455.handlers.NewsHandler;
import main.java.edu.upenn.cis.cis455.handlers.YelpHandler;

public class Server {
  public static void main(String[] args) {
    get("/hello", (req, res) -> "Hello World");
    get("/news", new NewsHandler());
    get("/yelp/:term/:location", new YelpHandler());
  }
}
