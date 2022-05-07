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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NewsHandler implements Route {
    private static final Logger logger = LogManager.getLogger(NewsHandler.class);

    private String apiKey;

    public NewsHandler() {
        try (InputStream in = NewsHandler.class.getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            prop.load(in);
            apiKey = prop.getProperty("news.apiKey");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config");
        }
    }

    @Override
    public Object handle(Request req, Response res) throws HaltException {
        String query = req.queryParams("query");
        String targetUrl = "https://newsapi.org/v2/everything?q=" + query + "&language=en";
        HttpURLConnection conn = null;
        String data = null;

        try {
            URL url = new URL(targetUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("X-Api-Key", apiKey);

            if (conn.getResponseCode() != 200) {
                logger.error("GET request to News API failed: {}", conn.getResponseCode());
                res.status(204);
			    return "No result from News";
            }

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder sb = new StringBuilder();
            String inputLine;

            while ((inputLine = br.readLine()) != null) {
                sb.append(inputLine);
            }

            br.close();
            data = sb.toString();
        } catch (Exception e) {
            logger.error("An error occurred:", e);
            res.status(500);
			return e.getMessage();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        res.type("application/json");
        return data;
    }
}