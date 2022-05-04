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

import edu.upenn.cis.cis455.Config;

public class YelpHandler implements Route {

    @Override
    public Object handle(Request req, Response res) throws HaltException {
        String term = req.params("term");
        String location = req.params("location");

        String targetUrl = "https://api.yelp.com/v3/businesses/search?term=" + term + "&location=" + location
                + "&limit=50";
        HttpURLConnection conn = null;
        String data = null;

        try {
            URL url = new URL(targetUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Authorization", "Bearer " + Config.YELP_API_KEY);

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("GET request to Yelp API failed: "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));
            StringBuilder sb = new StringBuilder();
            String inputLine;

            while ((inputLine = br.readLine()) != null) {
                sb.append(inputLine);
            }

            br.close();
            data = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        return data;
    }
}