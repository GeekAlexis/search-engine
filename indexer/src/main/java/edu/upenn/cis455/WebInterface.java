package edu.upenn.cis455;

import static spark.Spark.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;


public class WebInterface {
    

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Syntax: WebInterface {port}");
            System.exit(1);
        }

        setLevel("edu.upenn.cis455", Level.DEBUG);
        port(Integer.parseInt(args[0]));

        get("/retrieve", (req, res) -> {
            return null;
        });

    }
}
