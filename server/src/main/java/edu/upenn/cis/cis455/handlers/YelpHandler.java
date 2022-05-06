package edu.upenn.cis.cis455.handlers;

import spark.Request;
import spark.Route;
import spark.Response;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import spark.HaltException;

public class YelpHandler implements Route {
    private String apiKey;

    public YelpHandler() {
        try (InputStream in = YelpHandler.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            prop.load(in);
            apiKey = prop.getProperty("yelp.apiKey");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
            conn.setRequestProperty("Authorization", "Bearer " + apiKey);

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