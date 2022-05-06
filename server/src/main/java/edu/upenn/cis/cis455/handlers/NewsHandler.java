package edu.upenn.cis.cis455.handlers;

import spark.Request;
import spark.Route;
import spark.Response;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.InputStream;

import spark.HaltException;

public class NewsHandler implements Route {
    private String apiKey;

    public NewsHandler() {
        try (InputStream in = NewsHandler.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            prop.load(in);
            apiKey = prop.getProperty("news.apiKey");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object handle(Request req, Response res) throws HaltException {
        res.type("application/json");

        String query = req.queryParams("query");
        String targetUrl = "https://newsapi.org/v2/everything?q=" + query + "&language=en";
        HttpURLConnection conn = null;
        String data = "";

        try {
            URL url = new URL(targetUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("X-Api-Key", apiKey);

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("GET request to News API failed: "
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