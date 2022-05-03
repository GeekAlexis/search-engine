package main.java.edu.upenn.cis.cis455.handlers;

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

public class SearchHandler implements Route {
  @Override
  public Object handle(Request req, Response res) throws HaltException {
    String query = req.queryParams("query");

    return "";
  }
}
