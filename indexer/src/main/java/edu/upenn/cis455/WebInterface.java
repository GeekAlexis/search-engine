package edu.upenn.cis455;

import static spark.Spark.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import static org.apache.logging.log4j.core.config.Configurator.setLevel;

public class WebInterface {
    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Syntax: WebInterface {path} {root}");
            System.exit(1);
        }

        setLevel("edu.upenn.cis.cis455", Level.DEBUG);
    }
}
