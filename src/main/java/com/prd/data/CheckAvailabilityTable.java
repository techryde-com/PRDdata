package com.prd.data;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.testng.annotations.Test;

public class CheckAvailabilityTable {

	
	public static String url_FishandFire="jdbc:sqlserver://172.16.1.27;encrypt=false;user=FireFish;password=F!$hF!re#;database=FishandFireDB;integratedSecurity=false;";
	public static String url_sunflower="jdbc:sqlserver://172.16.1.14;encrypt=false;user=SunFlower;password=$Flower;database=Sunflower_OLO;integratedSecurity=false;";
	public static String url_Boncheur_OLO="jdbc:sqlserver://172.16.1.14;encrypt=false;user=Bonheur;password=Bonch@82;database=Boncheur_OLO;integratedSecurity=false;";
	public static String url_Weather_Gage_OLO="jdbc:sqlserver://172.16.1.14;encrypt=false;user=Weather;password=Gage@21;database=Weather_Gage_OLO;integratedSecurity=false;";
	public static String url_ALDOS_PRD_OLO="jdbc:sqlserver://172.16.1.12;encrypt=false;user=Aldos;password=Ald0$;database=ALDOS_PRD_OLO;integratedSecurity=false;";
	public static String url_Robertos_PRD_OLO="jdbc:sqlserver://172.16.1.11;encrypt=false;user=Robert;password=R0bert0$;database=Robertos_PRD_OLO;integratedSecurity=false;";
	public static String url_Kaminski_PRD_OLO="jdbc:sqlserver://172.16.1.11;encrypt=false;user=Kamin;password=K@m!n;database=Kaminski_PRD_OLO;integratedSecurity=false;";
	public static String url_Broadwaysdiner="jdbc:sqlserver://172.16.1.11;encrypt=false;user=Broadways;password=Br0adW;database=Broadwaysdiner;integratedSecurity=false;";
	public static String url_Ventura_PRD_OLO="jdbc:sqlserver://172.16.1.14;encrypt=false;user=Ventura;password=Venyur@2;database=Ventura_PRD_OLO;integratedSecurity=false;";
	public static String url_ShoreBreakPRD="jdbc:sqlserver://172.16.1.9;encrypt=false;user=SBreak;password=$h0rBreak;database=ShoreBreakPRD;integratedSecurity=false;";
	public static String url_Hapisgah_Steakhouse_PRD="jdbc:sqlserver://172.16.1.13;encrypt=false;user=Steakhouse;password=H$teak#0use;database=Hapisgah_Steakhouse_PRD;integratedSecurity=false;";
	public static String url_CosMos_PRD_OLO="jdbc:sqlserver://172.16.1.11;encrypt=false;user=CosMos;password=C0$M0$@1;database=CosMos_PRD_OLO;integratedSecurity=false;";
	public static String url_BB_Jacks_OLO="jdbc:sqlserver://172.16.1.13;encrypt=false;user=Jacks;password=Jacks;database=BB_Jacks_OLO;integratedSecurity=false;";
	public static String url_Billis_Bar_OLO="jdbc:sqlserver://172.16.1.13;encrypt=false;user=BBar;password=BBar;database=Billis_Bar_OLO;integratedSecurity=false;";
	public static String url_Teixeria="jdbc:sqlserver://172.16.1.12;encrypt=false;user=TaxiBaker;password=T@x!Baker;database=Taxieria Baker;integratedSecurity=false;";
	public static String url_cnpizza="jdbc:sqlserver://172.16.1.12;encrypt=false;user=qorder;password=qorder;database=COUNTRY_PIZZA_OLO;integratedSecurity=false;";
	public static String url_romapizza="jdbc:sqlserver://172.16.1.14;encrypt=false;user=roma_pizza;password=Techryde@123;database=ROMAPIZZA_PRD_OLO;integratedSecurity=false;";
	public static String url_wardroom="jdbc:sqlserver://172.16.1.14;encrypt=false;user=qorder;password=qorder;database=WARD_PRD_OLO;integratedSecurity=false;";

		public static String queryAvailability = "SELECT CreatedOn FROM StoreMenuItemAvailability";
		
		
		

	public static com.prd.utilities.Email_sender es = new com.prd.utilities.Email_sender();

	@Test
	public void runner() {

		checkAvailability("FishandFire", url_FishandFire, queryAvailability);
		checkAvailability("sunflower", url_sunflower, queryAvailability);
		checkAvailability("Boncheur_OLO", url_Boncheur_OLO, queryAvailability);
		checkAvailability("Weather_Gage_OLO", url_Weather_Gage_OLO, queryAvailability);
		checkAvailability("ALDOS_PRD_OLO", url_ALDOS_PRD_OLO, queryAvailability);
		checkAvailability("Robertos_PRD_OLO", url_Robertos_PRD_OLO, queryAvailability);
		checkAvailability("Kaminski_PRD_OLO", url_Kaminski_PRD_OLO, queryAvailability);
		checkAvailability("Broadwaysdiner", url_Broadwaysdiner, queryAvailability);
		checkAvailability("Ventura_PRD_OLO", url_Ventura_PRD_OLO, queryAvailability);
		checkAvailability("ShoreBreakPRD", url_ShoreBreakPRD, queryAvailability);
		checkAvailability("Hapisgah_Steakhouse_PRD", url_Hapisgah_Steakhouse_PRD, queryAvailability);
		checkAvailability("CosMos_PRD_OLO", url_CosMos_PRD_OLO, queryAvailability);
		checkAvailability("BB_Jacks_OLO", url_BB_Jacks_OLO, queryAvailability);
		checkAvailability("Billis_Bar_OLO", url_Billis_Bar_OLO, queryAvailability);
		checkAvailability("cnpizza", url_cnpizza, queryAvailability);
		checkAvailability("Teixeria", url_Teixeria, queryAvailability);
		checkAvailability("RomaPizza", url_romapizza, queryAvailability);
		checkAvailability("Wardroom", url_wardroom, queryAvailability);

	}

	public static boolean checkAvailability(String store, String url, String query) {

		boolean storeAvailabilityWorking = true;
		long minutesDifference = 0;

		try (Connection connection = DriverManager.getConnection(url);
				PreparedStatement statement = connection.prepareStatement(query);
				ResultSet resultSet = statement.executeQuery()) {

			// Get the server's current time in the Eastern Standard Time (EST) zone
			LocalDateTime currentDateTimeOnServer = LocalDateTime.now(ZoneId.of("America/New_York"));

			boolean recordsFound = false;

			while (resultSet.next()) {
				recordsFound = true;

				java.sql.Timestamp createdOnTimestamp = resultSet.getTimestamp("CreatedOn");

				if (createdOnTimestamp != null) {
					ZonedDateTime createdOnZonedDateTime = ZonedDateTime.ofInstant(createdOnTimestamp.toInstant(),
							ZoneId.systemDefault());
					LocalDateTime createdOnLocalDateTime = createdOnZonedDateTime.toLocalDateTime();

					Duration difference = Duration.between(createdOnLocalDateTime, currentDateTimeOnServer);
					minutesDifference = difference.toMinutes();

					if (minutesDifference > 2880) {
						storeAvailabilityWorking = false;
						break;
					} else {
						storeAvailabilityWorking = true;
					}
				}
			}

			if (!recordsFound) {
				System.out.println("No Availability Record");
			} else {
				System.out.println("store checking completed");
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return storeAvailabilityWorking;
	}

}
