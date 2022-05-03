package storage;

import java.sql.Connection;
import java.sql.DriverManager;

// Set up database connection to AWS RDS Postgresql
public class StorageFactory {
	
	private static StorageInterface instance = null;
	
    public static void connectToDatabase() {

        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://database-1.cnw1rlie1jes.us-east-1.rds.amazonaws.com:5432/postgresdb",
                            "postgres", "cis555db");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");

        instance = new StorageImpl(c);

    }
    
    public static StorageInterface getInstance() {
    	return instance;
    }

}
