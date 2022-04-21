package storage;

import java.sql.Connection;
import java.sql.DriverManager;

public class StorageFactory {
	
	private static StorageInterface instance = null;
	
    public static void connectToDatabase() {
        // TODO: factory object, instantiate your storage server

        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/postgres",
                            "postgres", "password");
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
