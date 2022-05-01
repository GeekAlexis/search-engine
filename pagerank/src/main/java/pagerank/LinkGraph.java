//package pagerank;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.Statement;
//import java.util.HashMap;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.PreparedStatement;
//import org.jsoup.nodes.*;
//import org.jsoup.select.Elements;
//import org.jsoup.Jsoup;
//
//
//public class LinkGraph {
//	
//	public static void main(String[] args) throws Exception {
//		
//        Connection c = connectDB();
//        Statement stmt = null;
//        
//        try {
//            stmt = c.createStatement();
//            String sql = "SELECT * FROM \"Document\" WHERE id <= 200;";
//            ResultSet rs = stmt.executeQuery(sql);
//
//            while (rs.next()) {
//                String url = rs.getString("url");
//                byte[] bytes = rs.getBytes("content");
//                String content = new String(bytes);
//                
//                // Extract hyperlinks and anchor tags
//                Document doc = Jsoup.parse(content);
//                Elements links = doc.select("a");
//                
//                for (int i = 0; i < links.size(); i++) {
//                	Element link = links.get(i);
//                	String linkHref = link.attr("href");
//                    String linkText = link.text();  
//                    
//                    if (!linkHref.isEmpty()) {
//	                    if (linkHref.charAt(0) == '#') {
//	                    	linkHref = url + '/' + linkHref;
//	                    } else {
//	                    	if (linkHref.length() >= 2) {
//	                    		if (linkHref.charAt(0) == '/' && linkHref.charAt(1) != '/') {
//	                    			linkHref = url + linkHref;
//	                    		}
//	                    	}
//	                    }
//	                    
//	                    if (linkHref.charAt(linkHref.length() - 1) == '/') {
//	                    	linkHref = linkHref.substring(0, linkHref.length() - 1);
//	                    }
//	                    
//	                    if (!linkHref.isEmpty()) {
//	                    	updateGraph(c, url, linkHref, linkText);
//	                    }
//                    }
//                }   
//            }
//            
//            rs.close();
//            stmt.close();  
//            
//            computeOutDegree(c);
//            
//            
//            
//            c.close();
//            
//            
//            
//            
//            
//            
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println(e.getClass().getName()+": "+e.getMessage());
//            System.exit(0);
//        }
//        
//        System.out.println("Finished");
//        
//    }
//	
//	public static Connection connectDB() {
//		// Set up database connection to AWS RDS Postgresql
//		Connection c = null;
//		
//        try {
//            Class.forName("org.postgresql.Driver");
//            c = DriverManager
//                    .getConnection("jdbc:postgresql://database-1.cnw1rlie1jes.us-east-1.rds.amazonaws.com:5432/postgresdb",
//                            "postgres", "cis555db");
//            
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println(e.getClass().getName()+": "+e.getMessage());
//            System.exit(0);
//        }
//        
//        System.out.println("Opened database successfully");
//        return c;
//	}
//	
//	public static boolean checkUrl(Connection c, String url, String table) {
//		// Check if the given url exists in db
//        Statement stmt = null;
//        
//        try {
//            stmt = c.createStatement();
//            String sql = "SELECT url FROM " + "\"" + table + "\"" + " WHERE url = '" + url + "';";
//            ResultSet rs = stmt.executeQuery(sql);
//
//            if (rs.next() == false){
//            	stmt.close();
//            	rs.close();
//            	return false;
//            } else {
//            	stmt.close();
//            	rs.close();
//            	return true;
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//            System.out.println("checkUrl failed");
//            System.exit(0);
//        }
//        
//        return false;
//	}
//	
//	public static void updateGraph(Connection c, String url, String link, String tag) {
//		
//        try {
//            String stmt = "INSERT INTO \"midgraph\" (url, link, tag, out) VALUES (?,?,?,?);";
//            PreparedStatement pst = c.prepareStatement(stmt);
//            pst.setString(1, url);
//            pst.setString(2, link);
//            pst.setString(3, tag);
//            pst.setInt(4, 1);
//            pst.executeUpdate();
//            pst.close();
//
//            if (!checkUrl(c, link, "Document") && !checkUrl(c, link, "midgraph")) {
//            	stmt = "INSERT INTO \"midgraph\" (url, link, tag) VALUES (?,?,?);";
//                PreparedStatement pst2 = c.prepareStatement(stmt);
//                pst2.setString(1, link);
//                pst2.setString(2, "");
//                pst2.setString(3, "");
//                
//                pst2.executeUpdate();                
//                pst2.close();
//            }
//            
//            System.out.println("Successfully add to graph");
//        } catch (SQLException e) {
//            e.printStackTrace();
//            System.out.println("Update graph failed");
//            System.exit(0);
//        }
//	}
//	
//	public static void computeOutDegree(Connection c) {
//		try {
//			c.setAutoCommit(false);
//			Statement stmt = c.createStatement();
//			stmt.setFetchSize(500);
//            String sql = "SELECT url, SUM (out) FROM \"midgraph\" GROUP BY url;";
//            ResultSet rs = stmt.executeQuery(sql);
//            
//            while (rs.next()) {
//                String url = rs.getString("url");
//                int num = rs.getInt("sum");
//                
//                sql = "INSERT INTO \"middegree\" (url, out) VALUES (?,?);";
//                PreparedStatement pst = c.prepareStatement(sql);
//                pst.setString(1, url);
//                pst.setInt(2, num);
//                pst.executeUpdate();                
//                pst.close();
//            }
//            
//            rs.close();
//            stmt.close();
//            c.setAutoCommit(true);
//            
//            System.out.println("Successfully compute the out degree");
//            
//        } catch (SQLException e) {
//            e.printStackTrace();
//            System.out.println("Compute out degree failed");
//            System.exit(0);
//        }
//		
//	}
//	
//	
//	
//	
//
//	
//
//}
