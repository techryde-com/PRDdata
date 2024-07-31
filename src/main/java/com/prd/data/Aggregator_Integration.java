package com.prd.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prd.utilities.Email_sender;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;




public class Aggregator_Integration {
	
	
	public static Email_sender es = new Email_sender();
	static List<String> failureMessagesUber = new ArrayList<>();
	
	public static boolean Aggregators(String connectionString ,String StoreName) {
        String querypm = "select * from tbl_PostmatesNotification where EventType like '%Uber%' and TransactionType like '%Menu Sync%' and BDate>= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()) - 1, 0) AND bdate < CAST(GETDATE() AS DATE)  order by TransactionId desc";
        String query1pm = "select * from tbl_PostmatesNotification where EventType like '%food%' and TransactionType like '%Catalog%' and BDate>= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()) - 1, 0) AND bdate < CAST(GETDATE() AS DATE)  order by TransactionId desc";
        boolean Aggregator_sync = true;
        
        //String query = "select * from tbl_PostmatesNotification where EventType like '%Uber%' and TransactionType like '%Menu Sync%' and BDate= '2023-12-08'order by TransactionId desc";
        try (Connection connection = DriverManager.getConnection(connectionString);
             PreparedStatement statement = connection.prepareStatement(querypm);
             ResultSet resultSet = statement.executeQuery()) {

        	
        	ObjectMapper objectMapper = new ObjectMapper();

            while (resultSet.next()) {
                int transactionId = resultSet.getInt("TransactionId");
                String jsonResponse = resultSet.getString("JsonResponse");

                // Check if the Json response contains "quota exceeded" or "invalid item"
                if (jsonResponse != null && (jsonResponse.contains("quota exceeded") || jsonResponse.contains("invalid item"))) {
                    try {
                        // Parse the JSON response
                        JsonNode jsonNode = objectMapper.readTree(jsonResponse);
                        String message = jsonNode.has("message") ? jsonNode.get("message").asText() : "Unknown Error";
                       failureMessagesUber.add("Menu sync Failed for TransactionId " + transactionId + " due to: " + message);
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    
                } 
                
                else {
                    // Process the retrieved data as needed
                    System.out.println("TransactionId: " + transactionId);
                    String jsonRequest = resultSet.getString("JsonRequest");
                    String transactionType = resultSet.getString("TransactionType");
                    String eventType = resultSet.getString("EventType");
                    java.sql.Date bDate = resultSet.getDate("Bdate");

                    // Print other details or process as needed
                    System.out.println("JsonRequest: " + jsonRequest);
                    System.out.println("TransactionType: " + transactionType);
                    System.out.println("EventType: " + eventType);
                    System.out.println("Bdate: " + bDate);
                    System.out.println("-----------------------");
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        
        try (Connection connection = DriverManager.getConnection(connectionString);
             PreparedStatement statement = connection.prepareStatement(query1pm);
             ResultSet resultSet1 = statement.executeQuery()) {

        	List<String> failureMessages = new ArrayList<>();
        	ObjectMapper objectMapper = new ObjectMapper();

            while (resultSet1.next()) {
                int transactionId = resultSet1.getInt("TransactionId");
                String jsonResponse = resultSet1.getString("JsonResponse");

                // Check if the Json response contains "quota exceeded" or "invalid item"
                if (jsonResponse != null && (jsonResponse.contains("exception") || jsonResponse.contains("false"))) {
                    try {
                        // Parse the JSON response
                        JsonNode jsonNode = objectMapper.readTree(jsonResponse);
                        String message = jsonNode.has("responseMessage") ? jsonNode.get("responseMessage").asText() : "Unknown Error";

                     failureMessages.add("Menu sync Failed for TransactionId " + transactionId + " due to: " + message);
                        
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    
                } 
                
                else {
                    // Process the retrieved data as needed
                    System.out.println("TransactionId: " + transactionId);
                    String jsonRequest = resultSet1.getString("JsonRequest");
                    String transactionType = resultSet1.getString("TransactionType");
                    String eventType = resultSet1.getString("EventType");
                    java.sql.Date bDate = resultSet1.getDate("Bdate");

                    // Print other details or process as needed
                    System.out.println("JsonRequest: " + jsonRequest);
                    System.out.println("TransactionType: " + transactionType);
                    System.out.println("EventType: " + eventType);
                    System.out.println("Bdate: " + bDate);
                    System.out.println("-----------------------");
                }
            }
            
          if(!failureMessagesUber.isEmpty() )  
          {
        	 
        	  Aggregator_sync=false;  
        	  String emailstring=String.join("\n",failureMessagesUber);
        	  String emailstring1 = String.join("\n",failureMessages);
        	  System.out.println(emailstring1);
        	  es.mailsender("UberEats\nUberEats Menu sync failed for: "+StoreName+" \nPlease review \n"+emailstring+"\n\n\nFoodPanda"+emailstring1);
        	  

          }
          else
          {
        	  Aggregator_sync=true;
          }
          

        } catch (SQLException e) {
            e.printStackTrace();
        }
	
     System.out.println(Aggregator_sync);   
     return Aggregator_sync;   
       
       
    }
	

        
 }

 



