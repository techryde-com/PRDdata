package com.qa.transactions;

import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class MonthlyTransactions {

	public static void main(String[] args) {
		List<String> jdbcUrls = Arrays.asList(
				"jdbc:sqlserver://172.16.1.27;encrypt=false;user=FireFish;password=F!$hF!re#;database=FishandFireDB;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.14;encrypt=false;user=SunFlower;password=$Flower;database=Sunflower_OLO;integratedSecurity=false;",
				// "jdbc:sqlserver://172.16.1.14;encrypt=false;user=Bonheur;password=Bonch@82;database=Boncheur_OLO;integratedSecurity=false;",
				// "jdbc:sqlserver://172.16.1.14;encrypt=false;user=Weather;password=Gage@21;database=Weather_Gage_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.12;encrypt=false;user=Aldos;password=Ald0$;database=ALDOS_PRD_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.11;encrypt=false;user=Robert;password=R0bert0$;database=Robertos_PRD_OLO;integratedSecurity=false;",
				// "jdbc:sqlserver://172.16.1.11;encrypt=false;user=Kamin;password=K@m!n;database=Kaminski_PRD_OLO;integratedSecurity=false;",
				// "jdbc:sqlserver://172.16.1.11;encrypt=false;user=Broadways;password=Br0adW;database=Broadwaysdiner;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.14;encrypt=false;user=Ventura;password=Venyur@2;database=Ventura_PRD_OLO;integratedSecurity=false;",
				// "jdbc:sqlserver://172.16.1.9;encrypt=false;user=SBreak;password=$h0rBreak;database=ShoreBreakPRD;integratedSecurity=false;",
				// "jdbc:sqlserver://172.16.1.15;encrypt=false;user=Ananda;password=An@nd@;database=Ananda_PRD_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.13;encrypt=false;user=Steakhouse;password=H$teak#0use;database=Hapisgah_Steakhouse_PRD;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.11;encrypt=false;user=CosMos;password=C0$M0$@1;database=CosMos_PRD_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.13;encrypt=false;user=Jacks;password=Jacks;database=BB_Jacks_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.13;encrypt=false;user=BBar;password=BBar;database=Billis_Bar_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.12;encrypt=false;user=TaxiBaker;password=T@x!Baker;database=Taxieria Baker;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.12;encrypt=false;user=qorder;password=qorder;database=COUNTRY_PIZZA_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.14;encrypt=false;user=roma_pizza;password=Techryde@123;database=ROMAPIZZA_PRD_OLO;integratedSecurity=false;",
				"jdbc:sqlserver://172.16.1.14;encrypt=false;user=qorder;password=qorder;database=WARD_PRD_OLO;integratedSecurity=false;");

		String query = "SELECT DISTINCT POSCheckIdNumber, createdDatetime AS createdDate\n"
				+ "FROM OrderProcessorResponse\n" + "WHERE POSCheckIdNumber != '0'\n"
				+ "    AND POSCheckIdNumber != '0/0'\n"
				+ "    AND createdDatetime BETWEEN '2024-07-01' AND '2024-08-01'\n" + "ORDER BY createdDatetime asc;\n"
				+ "";
		String excelFilePath = "output.xlsx";

		try (XSSFWorkbook workbook = new XSSFWorkbook()) {
			for (String jdbcUrl : jdbcUrls) {
				String databaseName = extractDatabaseName(jdbcUrl);
				try (Connection connection = DriverManager.getConnection(jdbcUrl);
						Statement statement = connection.createStatement();
						ResultSet resultSet = statement.executeQuery(query)) {

					createExcelSheet(resultSet, workbook, databaseName);
					System.out.println("Transactions fetched for Store: " + databaseName);
				} catch (SQLException e) {
					System.err.println("Error fetching data for Store: " + databaseName);
					e.printStackTrace();
				}
			}

			try (FileOutputStream fileOut = new FileOutputStream(excelFilePath)) {
				workbook.write(fileOut);
				System.out.println("Excel file created successfully!");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void createExcelSheet(ResultSet resultSet, XSSFWorkbook workbook, String sheetName)
			throws SQLException {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

		org.apache.poi.ss.usermodel.Sheet sheet = workbook.createSheet(sheetName);

		// Create header row
		Row headerRow = sheet.createRow(0);
		headerRow.createCell(0).setCellValue("POSCheckIdNumber");
		headerRow.createCell(1).setCellValue("createdDate");

		// Populate data rows
		int rowNum = 1;
		while (resultSet.next()) {
			Row row = sheet.createRow(rowNum++);
			row.createCell(0).setCellValue(resultSet.getString("POSCheckIdNumber"));

			// Use the alias "createdDate" instead of "createdDatetime"
			Date createdDate = resultSet.getDate("createdDate");
			String formattedDate = (createdDate != null) ? dateFormat.format(createdDate) : "";
			row.createCell(1).setCellValue(formattedDate);
		}
	}

	private static String extractDatabaseName(String jdbcUrl) {

		int index = jdbcUrl.indexOf("database=");
		if (index != -1) {
			int endIndex = jdbcUrl.indexOf(";", index);
			return jdbcUrl.substring(index + 9, (endIndex != -1) ? endIndex : jdbcUrl.length());
		}
		return "UnknownDatabase"; // Default name if not found
	}
}