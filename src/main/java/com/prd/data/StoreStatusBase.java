package com.prd.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prd.utilities.Email_sender;

public class StoreStatusBase {
	public static Email_sender es = new Email_sender();
	public static String connectionString;
	public static String query;
	public static java.sql.Date date;
	public static String emailContent;
	public static int recordCountFromQuery;
	static int countfailure;
	static int recordCountDD;
	public static String emailstring;
	static String queryforAvailability = "SELECT CreatedOn FROM StoreMenuItemAvailability";
	public static CheckAvailabilityTable check = new CheckAvailabilityTable();
	public static Aggregator_Integration ai = new Aggregator_Integration();

	public static List<String> failureMessagesUber = new ArrayList<>();

	static int type1Count = 0;
	static int type2Count = 0;
	static int type3Count = 0;
	static int type4Count = 0;
	static int type5Count = 0;
	static int recordsAuth = 0;
	static int Order_Count=0;

	public static void StoreStatus(String Connection_String, String StoreName) {
		
			
			
		
			String connectionString = Connection_String;
			
			
			try (Connection connection = DriverManager.getConnection(connectionString)){
	            String QueryOrderCount = "SELECT COUNT(*) AS RecordCount\n"
	            		+ "FROM OrderProcessorResponse \n"
	            		+ "WHERE (POSCheckIdNumber != '0/0' AND POSCheckIdNumber != '0')  \n"
	            		+ "AND createdDatetime >= DATEADD(DAY, -6, GETDATE())";

	            PreparedStatement statement = connection.prepareStatement(QueryOrderCount);
	            ResultSet resultSet = statement.executeQuery();

	            if (resultSet.next()) {
	                Order_Count = resultSet.getInt("RecordCount");
	                System.out.println("Record count: " + Order_Count);
	            } else {
	                System.out.println("No records found.");
	            }
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
			
			
			System.out.println("nandan"+Order_Count);
			
			
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
	        	  emailstring=String.join("\n",failureMessagesUber);
	        	  //es.mailsender("UberEats\nUberEats Menu sync failed for: "+StoreName+" \nPlease review \n"+emailstring);
	        	  

	          }
	          else
	          {
	        	  Aggregator_sync=true;
	          }
	          

	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
		
	     System.out.println(Aggregator_sync);   
  
		

		connectionString = Connection_String;

		String sqlQueryDD = "SELECT Request, Response, CheckNo, TransactionType, BDate, id " + "FROM DoordashLogging "
				+ "WHERE BDate >= CAST(GETDATE() - 3 AS DATE) AND BDate < CAST(GETDATE() AS DATE) "
				+ "AND TransactionType = 'Create' AND CheckNo = ''";

		recordCountDD = 0;

		try (Connection connection = DriverManager.getConnection(connectionString);
				PreparedStatement preparedStatement = connection.prepareStatement(sqlQueryDD)) {

			// Execute the query
			ResultSet resultSet = preparedStatement.executeQuery();

			// Check if there are any results
			if (!resultSet.isBeforeFirst()) {
				// No results, print a message
				System.out.println("No Doordash Transactions for yesterday.");
			} else {
				// Process the result set
				while (resultSet.next()) {
					String request = resultSet.getString("Request");
					String response = resultSet.getString("Response");
					String checkNo = resultSet.getString("CheckNo");
					String transactionType = resultSet.getString("TransactionType");
					String bDate = resultSet.getString("BDate");
					int id = resultSet.getInt("id");

					// Process the fetched data as needed
					System.out.println("Request: " + request + ", Response: " + response + ", CheckNo: " + checkNo
							+ ", TransactionType: " + transactionType + ", BDate: " + bDate + ", id: " + id);

					recordCountDD++;
				}

				System.out.println("Total records returned: " + recordCountDD);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println("Doordash table checking completed");

		// Use DATEDIFF to fetch records at least two days ago from the current date
		// String queryAuth = "SELECT * FROM tbl_i4goTransactions WHERE OrderNo != ''
		// AND OrderNo!='0/0' AND Status = 'AUTH' AND DATEDIFF(DAY, bdate, GETDATE()) >=
		// 2 ORDER BY bdate DESC";
		String queryAuth = "SELECT * \r\n" + "FROM tbl_i4goTransactions \r\n" + "WHERE OrderNo != '' \r\n"
				+ "      AND OrderNo != '0/0' \r\n" + "      AND Status = 'AUTH' \r\n"
				+ "      AND DATEDIFF(DAY, bdate, GETDATE()) >= 2 \r\n"
				+ "      AND DATEDIFF(DAY, bdate, GETDATE()) <= 7 \r\n" + "ORDER BY bdate DESC;\r\n" + "";

		recordsAuth = 0;

		try {
			// Load the JDBC driver
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			// Establish a connection
			Connection connection = DriverManager.getConnection(connectionString);

			// Prepare the SQL query
			PreparedStatement preparedStatement = connection.prepareStatement(queryAuth);

			// Execute the query
			ResultSet resultSet = preparedStatement.executeQuery();

			// Process the result set and count records
			while (resultSet.next()) {
				// Increment the record count for each retrieved record
				recordsAuth++;

				// Retrieve data from the result set
				String request = resultSet.getString("Request");
				String response = resultSet.getString("Response");
				String bdate = resultSet.getString("BDate");
				String status = resultSet.getString("Status");
				String orderNo = resultSet.getString("OrderNo");

				// Print or process the retrieved data
				System.out.println(", BDate: " + bdate + ", Status: " + status + ", OrderNo: " + orderNo);
			}

			// Print or use the record count
			System.out.println("Total records: " + recordsAuth);

		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

		// Updated Database connection parameters

		String sqlQuery = "SELECT * \r\n"
				+ "FROM tbl_i4goTransactions \r\n"
				+ "WHERE BDate >= CAST(GETDATE() - 2 AS DATE) \r\n"
				+ "  AND BDate < CAST(GETDATE() AS DATE)\r\n"
				+ "  AND ISJSON(Response) > 0\r\n"
				+ "  AND JSON_QUERY(Response, '$.result[0].amount') IS NOT NULL;";

		try (Connection connection = DriverManager.getConnection(connectionString);
				PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
				ResultSet resultSet = preparedStatement.executeQuery()) {

			// Variables to store segregated records
			type1Count = 0;
			type2Count = 0;
			type3Count = 0;
			type4Count = 0;
			type5Count = 0;
			// Fetching and segregating the records
			while (resultSet.next()) {
				String status = resultSet.getString("Status");
				String orderNo = resultSet.getString("OrderNo");
				String InvoiceId = resultSet.getString("InvoiceId");
				String response = resultSet.getString("Response");
				date = resultSet.getDate("BDate");
				List<String> InvalidSales = new ArrayList<>();
				List<String> failedOrderAuth = new ArrayList<>();
				List<String> Authorised = new ArrayList<>();
				List<String> VoidAfterSales = new ArrayList<>();
				List<String> Declined = new ArrayList<>();

				// Condition for Type 1: status is 'SALE' and OrderNo is not empty
				if ("SALE".equals(status) && ("0".equals(orderNo) || "0/0".equals(orderNo) || orderNo.isEmpty())) {

					type1Count++;
					InvalidSales.add(InvoiceId);

				}

				else if ("AUTH".equals(status) && orderNo.isEmpty()
						&& !("D".equals(getResponseCodeFromJson(response)))) {

					type2Count++;
					failedOrderAuth.add(InvoiceId);

				}

				else if ("AUTH".equals(status)
						&& !("0".equals(orderNo) || "0/0".equals(orderNo) || orderNo.isEmpty())) {

					type3Count++;
					Authorised.add(InvoiceId);

				} else if ("VOID".equals(status)
						&& !("0".equals(orderNo) || "0/0".equals(orderNo) || orderNo.isEmpty())) {
					type4Count++;
					VoidAfterSales.add(InvoiceId);
				} else if ("AUTH".equals(status) && "D".equals(getResponseCodeFromJson(response))) {
					type5Count++;
					Declined.add(InvoiceId);

				}
			}

			countfailure = type1Count + type2Count + type4Count + type5Count;

			System.out.println("Invalid Orders converted to SALE :" + type1Count);
			System.out.println("Failed Orders but payment completed :" + type2Count);
			System.out.println("Authorised payments :" + type3Count);
			System.out.println("Void after successful payment :" + type4Count);
			System.out.println("Declined Payments : " + type5Count);

			System.out.println(countfailure);

		}

		catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println("Transaction Tables checking completed");

		connectionString = Connection_String;
		query = "WITH orders AS (\r\n"
				+ "    SELECT\r\n"
				+ "        OP.OrderId,\r\n"
				+ "        OP.OrderRequestJson,\r\n"
				+ "        OP.UniQueOrderIdentifier,\r\n"
				+ "        OP.createdDatetime,\r\n"
				+ "        OPR.OrderResponseJson,\r\n"
				+ "        OPR.POSCheckIdNumber\r\n"
				+ "    FROM\r\n"
				+ "        OrderProcessor OP\r\n"
				+ "    JOIN\r\n"
				+ "        OrderProcessorResponse OPR ON OP.OrderId = OPR.OrderId\r\n"
				+ " \r\n"
				+ "     \r\n"
				+ ")\r\n"
				+ "\r\n"
				+ "SELECT *\r\n"
				+ "FROM orders\r\n"
				+ "WHERE createdDatetime >= CAST(GETDATE() - 1 AS DATE)\r\n"
				+ "  AND createdDatetime < CAST(GETDATE() AS DATE);\r\n"
				+ "";

		List<Order> successfulOrders = new ArrayList<>();
		List<Order> failedOrders = new ArrayList<>();
		List<String> failed_messages = new ArrayList<>();

		try (Connection connection = DriverManager.getConnection(connectionString);
				PreparedStatement preparedStatement = connection.prepareStatement(query);
				ResultSet resultSet = preparedStatement.executeQuery()) {

			while (resultSet.next()) {
				int orderId = resultSet.getInt("OrderId");
				String orderRequestJson = resultSet.getString("OrderRequestJson");
				String uniqueOrderIdentifier = resultSet.getString("UniQueOrderIdentifier");
				Date createdDatetime = resultSet.getTimestamp("createdDatetime");
				String orderResponseJson = resultSet.getString("OrderResponseJson");
				String posCheckIdNumber = resultSet.getString("POSCheckIdNumber");

				// Create an Order object
				Order order = new Order(orderId, orderRequestJson, uniqueOrderIdentifier, createdDatetime,
						orderResponseJson, posCheckIdNumber);

				// Filter condition for successful orders
				if (isValidPosCheckIdNumber(posCheckIdNumber)) {
					successfulOrders.add(order);

				} else {
					failedOrders.add(order);
					orderResponseJson = resultSet.getString("OrderResponseJson");
					String ErrorMessage = extractErrorMessage(orderResponseJson);
					System.out.println(ErrorMessage);
					failed_messages.add("\n" + orderId + ":" + ErrorMessage);

				}
			}

			System.out.println("Store Name: " + StoreName);
			// System.out.println("Date: "+formatDate(createdDatetime));

			System.out.println("Successful OrderIds:");
			printOrderIds(successfulOrders);

			System.out.println("Failed OrderIds:");
			printOrderIds(failedOrders);

			// Print the number of passed, failed, and total orders

			System.out.println("\nTotal Number of Orders: " + (successfulOrders.size() + failedOrders.size()));
			System.out.println("Number of Passed Orders: " + successfulOrders.size());
			System.out.println("Number of Failed Orders: " + failedOrders.size());

			emailContent = generateEmailContent(successfulOrders, failedOrders, failed_messages);

		} catch (SQLException e) {
			e.printStackTrace();
		}

		System.out.println("Order Checking completed");

		boolean sync = CheckAvailabilityTable.checkAvailability(StoreName, connectionString, queryforAvailability);
	

		System.out.println("Menu syncing checking completed & Aggregator Integration Checked");
		

		if (countfailure > 0 || !failedOrders.isEmpty() || sync == false || recordsAuth != 0 || type5Count != 0
				|| recordCountDD != 0 || Order_Count==0) {

			es.mailsender("Store Name : " + StoreName + "\nDate : " + date + "\nInvalid Orders converted to SALE : "
					+ type1Count + "\nFailed Orders but payment completed : " + type2Count
					+ "\nVoid after successful payment : " + type4Count + "\n" + emailContent + "\nSync Working: "
					+ sync + "\nOld records not converted to sale : " + recordsAuth + "\nDeclined Payments : "
					+ type5Count + "\nDoorDash failed orders : " + recordCountDD +"\nAggregator intgration\n"+"UberEats\nUberEats Menu sync : "+Aggregator_sync+ "\nError : "+emailstring+"\nOrder count in last 2 days = "+Order_Count);

		}

	}

	// Helper method to check validity of POSCheckIdNumber
	private static boolean isValidPosCheckIdNumber(String posCheckIdNumber) {
		return posCheckIdNumber != null && !posCheckIdNumber.trim().isEmpty() && !posCheckIdNumber.equals("0")
				&& !posCheckIdNumber.equals("0/0");
	}

	// Helper method to print only the OrderId from a list of orders
	private static void printOrderIds(List<Order> orders) {
		for (Order order : orders) {
			System.out.println("OrderId: " + order.orderId);
		}
	}

	// Order class to represent a fetched order
	public static class Order {
		int orderId;
		String orderRequestJson;
		String uniqueOrderIdentifier;
		Date createdDatetime;
		String orderResponseJson;
		String posCheckIdNumber;

		public Order(int orderId, String orderRequestJson, String uniqueOrderIdentifier, Date createdDatetime,
				String orderResponseJson, String posCheckIdNumber) {
			this.orderId = orderId;
			this.orderRequestJson = orderRequestJson;
			this.uniqueOrderIdentifier = uniqueOrderIdentifier;
			this.createdDatetime = createdDatetime;
			this.orderResponseJson = orderResponseJson;
			this.posCheckIdNumber = posCheckIdNumber;
		}

	}

	private static String formatDate(Date date) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(date);
	}

	private static String generateEmailContent(List<Order> successfulOrders, List<Order> failedOrders,
			List<String> failed_messages) {
		// Build the email content as per your requirement
		StringBuilder contentBuilder = new StringBuilder();
		// contentBuilder.append("Store Name: ").append(StoreName).append("\n");
		// contentBuilder.append("\nSuccessful OrderIds:\n");
		// appendOrderIds(contentBuilder, successfulOrders);
		if (!(failedOrders.isEmpty())) {
			contentBuilder.append("\nFailed OrderIds:\n");
			appendOrderIds(contentBuilder, failedOrders);
			contentBuilder.append("\nFailure Reason" + failed_messages);
		}
		contentBuilder.append("\n\nNumber of Passed Orders: ").append(successfulOrders.size()).append("\n");
		contentBuilder.append("Number of Failed Orders: ").append(failedOrders.size()).append("\n");
		contentBuilder.append("Total Number of Orders: ").append(successfulOrders.size() + failedOrders.size())
				.append("\n");

		return contentBuilder.toString();
	}

	private static void appendOrderIds(StringBuilder builder, List<Order> orders) {
		for (Order order : orders) {
			builder.append("OrderId: ").append(order.orderId).append("\n");
		}
	}

	private static String extractErrorMessage(String orderResponseJson) {
		try {
			if (orderResponseJson != null && !orderResponseJson.isEmpty()) {
				JSONObject jsonObject = new JSONObject(orderResponseJson);

				// Navigate to the "pTotalsResponseEx" object
				JSONObject pTotalsResponseExObject = jsonObject.optJSONObject("pTotalsResponseEx");

				if (pTotalsResponseExObject != null) {
					// Navigate to the "OperationalResult" object
					JSONObject operationalResultObject = pTotalsResponseExObject.optJSONObject("OperationalResult");

					if (operationalResultObject != null) {
						// Retrieve the "ErrorMessage" value
						return operationalResultObject.optString("ErrorMessage", null);
					} else {
						System.out.println("OperationalResult object is null.");
					}
				} else {
					System.out.println("pTotalsResponseEx object is null.");
				}
			} else {
				System.out.println("orderResponseJson is null or empty.");
			}
		} catch (Exception e) {
			e.printStackTrace(); // Handle the exception appropriately
		}
		return null;
	}

	private static String getResponseCodeFromJson(String orderResponseJson) {
		try {
			if (orderResponseJson != null && !orderResponseJson.isEmpty()) {
				JSONObject jsonObject = new JSONObject(orderResponseJson);

				// Navigate to the "response" object
				JSONObject responseObject = jsonObject.optJSONObject("response");

				if (responseObject != null) {
					// Retrieve the "responseCode" value
					return responseObject.optString("responseCode", null);
				} else {
					System.out.println("response object is null.");
				}
			} else {
				System.out.println("orderResponseJson is null or empty.");
			}
		} catch (Exception e) {
			e.printStackTrace(); // Handle the exception appropriately
		}
		return null;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void StoreStatus2(String Connection_String, String StoreName) {
		
		
		
		
		String connectionString = Connection_String;
		
		
		try (Connection connection = DriverManager.getConnection(connectionString)){
            String QueryOrderCount = "SELECT COUNT(*) AS RecordCount\n"
            		+ "FROM OrderProcessorResponse \n"
            		+ "WHERE (POSCheckIdNumber != '0/0' AND POSCheckIdNumber != '0')  \n"
            		+ "AND createdDatetime >= DATEADD(DAY, -6, GETDATE())";

            PreparedStatement statement = connection.prepareStatement(QueryOrderCount);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                Order_Count = resultSet.getInt("RecordCount");
                System.out.println("Record count: " + Order_Count);
            } else {
                System.out.println("No records found.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
		
		
		System.out.println("nandan"+Order_Count);
		
		
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
        	  emailstring=String.join("\n",failureMessagesUber);
        	  //es.mailsender("UberEats\nUberEats Menu sync failed for: "+StoreName+" \nPlease review \n"+emailstring);
        	  

          }
          else
          {
        	  Aggregator_sync=true;
          }
          

        } catch (SQLException e) {
            e.printStackTrace();
        }
	
     System.out.println(Aggregator_sync);   

	

	connectionString = Connection_String;

	String sqlQueryDD = "SELECT Request, Response, CheckNo, TransactionType, BDate, id " + "FROM DoordashLogging "
			+ "WHERE BDate >= CAST(GETDATE() - 3 AS DATE) AND BDate < CAST(GETDATE() AS DATE) "
			+ "AND TransactionType = 'Create' AND CheckNo = ''";

	recordCountDD = 0;

	try (Connection connection = DriverManager.getConnection(connectionString);
			PreparedStatement preparedStatement = connection.prepareStatement(sqlQueryDD)) {

		// Execute the query
		ResultSet resultSet = preparedStatement.executeQuery();

		// Check if there are any results
		if (!resultSet.isBeforeFirst()) {
			// No results, print a message
			System.out.println("No Doordash Transactions for yesterday.");
		} else {
			// Process the result set
			while (resultSet.next()) {
				String request = resultSet.getString("Request");
				String response = resultSet.getString("Response");
				String checkNo = resultSet.getString("CheckNo");
				String transactionType = resultSet.getString("TransactionType");
				String bDate = resultSet.getString("BDate");
				int id = resultSet.getInt("id");

				// Process the fetched data as needed
				System.out.println("Request: " + request + ", Response: " + response + ", CheckNo: " + checkNo
						+ ", TransactionType: " + transactionType + ", BDate: " + bDate + ", id: " + id);

				recordCountDD++;
			}

			System.out.println("Total records returned: " + recordCountDD);
		}

	} catch (SQLException e) {
		e.printStackTrace();
	}

	System.out.println("Doordash table checking completed");

	// Use DATEDIFF to fetch records at least two days ago from the current date
	// String queryAuth = "SELECT * FROM tbl_i4goTransactions WHERE OrderNo != ''
	// AND OrderNo!='0/0' AND Status = 'AUTH' AND DATEDIFF(DAY, bdate, GETDATE()) >=
	// 2 ORDER BY bdate DESC";
	String queryAuth = "SELECT * \r\n" + "FROM tbl_i4goTransactions \r\n" + "WHERE OrderNo != '' \r\n"
			+ "      AND OrderNo != '0/0' \r\n" + "      AND Status = 'AUTH' \r\n"
			+ "      AND DATEDIFF(DAY, bdate, GETDATE()) >= 2 \r\n"
			+ "      AND DATEDIFF(DAY, bdate, GETDATE()) <= 7 \r\n" + "ORDER BY bdate DESC;\r\n" + "";

	recordsAuth = 0;

	try {
		// Load the JDBC driver
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

		// Establish a connection
		Connection connection = DriverManager.getConnection(connectionString);

		// Prepare the SQL query
		PreparedStatement preparedStatement = connection.prepareStatement(queryAuth);

		// Execute the query
		ResultSet resultSet = preparedStatement.executeQuery();

		// Process the result set and count records
		while (resultSet.next()) {
			// Increment the record count for each retrieved record
			recordsAuth++;

			// Retrieve data from the result set
			String request = resultSet.getString("Request");
			String response = resultSet.getString("Response");
			String bdate = resultSet.getString("BDate");
			String status = resultSet.getString("Status");
			String orderNo = resultSet.getString("OrderNo");

			// Print or process the retrieved data
			System.out.println(", BDate: " + bdate + ", Status: " + status + ", OrderNo: " + orderNo);
		}

		// Print or use the record count
		System.out.println("Total records: " + recordsAuth);

	} catch (ClassNotFoundException | SQLException e) {
		e.printStackTrace();
	}

	// Updated Database connection parameters

	String sqlQuery = "SELECT * \r\n"
			+ "FROM tbl_i4goTransactions \r\n"
			+ "WHERE BDate >= CAST(GETDATE() - 2 AS DATE) \r\n"
			+ "  AND BDate < CAST(GETDATE() AS DATE)\r\n"
			+ "  AND ISJSON(Response) > 0\r\n"
			+ "  AND JSON_QUERY(Response, '$.result[0].amount') IS NOT NULL;";

	try (Connection connection = DriverManager.getConnection(connectionString);
			PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
			ResultSet resultSet = preparedStatement.executeQuery()) {

		// Variables to store segregated records
		type1Count = 0;
		type2Count = 0;
		type3Count = 0;
		type4Count = 0;
		type5Count = 0;
		// Fetching and segregating the records
		while (resultSet.next()) {
			String status = resultSet.getString("Status");
			String orderNo = resultSet.getString("OrderNo");
			String InvoiceId = resultSet.getString("InvoiceId");
			String response = resultSet.getString("Response");
			date = resultSet.getDate("BDate");
			List<String> InvalidSales = new ArrayList<>();
			List<String> failedOrderAuth = new ArrayList<>();
			List<String> Authorised = new ArrayList<>();
			List<String> VoidAfterSales = new ArrayList<>();
			List<String> Declined = new ArrayList<>();

			// Condition for Type 1: status is 'SALE' and OrderNo is not empty
			if ("SALE".equals(status) && ("0".equals(orderNo) || "0/0".equals(orderNo) || orderNo.isEmpty())) {

				type1Count++;
				InvalidSales.add(InvoiceId);

			}

			else if ("AUTH".equals(status) && orderNo.isEmpty()
					&& !("D".equals(getResponseCodeFromJson(response)))) {

				type2Count++;
				failedOrderAuth.add(InvoiceId);

			}

			else if ("AUTH".equals(status)
					&& !("0".equals(orderNo) || "0/0".equals(orderNo) || orderNo.isEmpty())) {

				type3Count++;
				Authorised.add(InvoiceId);

			} else if ("VOID".equals(status)
					&& !("0".equals(orderNo) || "0/0".equals(orderNo) || orderNo.isEmpty())) {
				type4Count++;
				VoidAfterSales.add(InvoiceId);
			} else if ("AUTH".equals(status) && "D".equals(getResponseCodeFromJson(response))) {
				type5Count++;
				Declined.add(InvoiceId);

			}
		}

		countfailure = type1Count + type2Count + type4Count + type5Count;

		System.out.println("Invalid Orders converted to SALE :" + type1Count);
		System.out.println("Failed Orders but payment completed :" + type2Count);
		System.out.println("Authorised payments :" + type3Count);
		System.out.println("Void after successful payment :" + type4Count);
		System.out.println("Declined Payments : " + type5Count);

		System.out.println(countfailure);

	}

	catch (SQLException e) {
		e.printStackTrace();
	}

	System.out.println("Transaction Tables checking completed");

	connectionString = Connection_String;
	query = "WITH orders AS (\r\n"
			+ "    SELECT\r\n"
			+ "        OP.OrderId,\r\n"
			+ "        OP.OrderRequestJson,\r\n"
			+ "        OP.UniQueOrderIdentifier,\r\n"
			+ "        OP.createdDatetime,\r\n"
			+ "        OPR.OrderResponseJson,\r\n"
			+ "        OPR.POSCheckIdNumber\r\n"
			+ "    FROM\r\n"
			+ "        OrderProcessor OP\r\n"
			+ "    JOIN\r\n"
			+ "        OrderProcessorResponse OPR ON OP.OrderId = OPR.OrderId\r\n"
			+ "    WHERE\r\n"
			+ "        OP.UniQueOrderIdentifier LIKE '%OLO%'\r\n"
			+ "        OR OP.UniQueOrderIdentifier LIKE '%Uber%'\r\n"
			+ ")\r\n"
			+ "\r\n"
			+ "SELECT *\r\n"
			+ "FROM orders\r\n"
			+ "WHERE createdDatetime >= CAST(GETDATE() - 1 AS DATE)\r\n"
			+ "  AND createdDatetime < CAST(GETDATE() AS DATE);\r\n"
			+ "";

	List<Order> successfulOrders = new ArrayList<>();
	List<Order> failedOrders = new ArrayList<>();
	List<String> failed_messages = new ArrayList<>();

	try (Connection connection = DriverManager.getConnection(connectionString);
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			ResultSet resultSet = preparedStatement.executeQuery()) {

		while (resultSet.next()) {
			int orderId = resultSet.getInt("OrderId");
			String orderRequestJson = resultSet.getString("OrderRequestJson");
			String uniqueOrderIdentifier = resultSet.getString("UniQueOrderIdentifier");
			Date createdDatetime = resultSet.getTimestamp("createdDatetime");
			String orderResponseJson = resultSet.getString("OrderResponseJson");
			String posCheckIdNumber = resultSet.getString("POSCheckIdNumber");

			// Create an Order object
			Order order = new Order(orderId, orderRequestJson, uniqueOrderIdentifier, createdDatetime,
					orderResponseJson, posCheckIdNumber);

			// Filter condition for successful orders
			if (isValidPosCheckIdNumber(posCheckIdNumber)) {
				successfulOrders.add(order);

			} else {
				failedOrders.add(order);
				orderResponseJson = resultSet.getString("OrderResponseJson");
				String ErrorMessage = extractErrorMessage(orderResponseJson);
				System.out.println(ErrorMessage);
				failed_messages.add("\n" + orderId + ":" + ErrorMessage);

			}
		}

		System.out.println("Store Name: " + StoreName);
		// System.out.println("Date: "+formatDate(createdDatetime));

		System.out.println("Successful OrderIds:");
		printOrderIds(successfulOrders);

		System.out.println("Failed OrderIds:");
		printOrderIds(failedOrders);

		// Print the number of passed, failed, and total orders

		System.out.println("\nTotal Number of Orders: " + (successfulOrders.size() + failedOrders.size()));
		System.out.println("Number of Passed Orders: " + successfulOrders.size());
		System.out.println("Number of Failed Orders: " + failedOrders.size());

		emailContent = generateEmailContent(successfulOrders, failedOrders, failed_messages);

	} catch (SQLException e) {
		e.printStackTrace();
	}

	System.out.println("Order Checking completed");

	boolean sync = CheckAvailabilityTable.checkAvailability(StoreName, connectionString, queryforAvailability);


	System.out.println("Menu syncing checking completed & Aggregator Integration Checked");
	

	if (countfailure > 0 || !failedOrders.isEmpty() || sync == false || recordsAuth != 0 || type5Count != 0
			|| recordCountDD != 0 || Order_Count==0) {

		es.mailsender("Store Name : " + StoreName + "\nDate : " + date + "\nInvalid Orders converted to SALE : "
				+ type1Count + "\nFailed Orders but payment completed : " + type2Count
				+ "\nVoid after successful payment : " + type4Count + "\n" + emailContent + "\nSync Working: "
				+ sync + "\nOld records not converted to sale : " + recordsAuth + "\nDeclined Payments : "
				+ type5Count + "\nDoorDash failed orders : " + recordCountDD +"\nAggregator intgration\n"+"UberEats\nUberEats Menu sync : "+Aggregator_sync+ "\nError : "+emailstring+"\nOrder count in last 2 days = "+Order_Count);

	}

}

// Order class to represent a fetched order
public static class Order1 {
	int orderId;
	String orderRequestJson;
	String uniqueOrderIdentifier;
	Date createdDatetime;
	String orderResponseJson;
	String posCheckIdNumber;

	public Order1(int orderId, String orderRequestJson, String uniqueOrderIdentifier, Date createdDatetime,
			String orderResponseJson, String posCheckIdNumber) {
		this.orderId = orderId;
		this.orderRequestJson = orderRequestJson;
		this.uniqueOrderIdentifier = uniqueOrderIdentifier;
		this.createdDatetime = createdDatetime;
		this.orderResponseJson = orderResponseJson;
		this.posCheckIdNumber = posCheckIdNumber;
	}

}
	
	
	
	
	
}
