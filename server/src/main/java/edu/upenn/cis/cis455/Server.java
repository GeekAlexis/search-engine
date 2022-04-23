package edu.upenn.cis.cis455;

import static spark.Spark.*;

public class Server {
  public static void main(String[] args) {
    get("/hello", (req, res) -> "Hello World");
  }
}
