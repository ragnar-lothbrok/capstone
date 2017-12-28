package com.edureka.capstone;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProblemOne {

	private static final Logger logger = LoggerFactory.getLogger(ProblemOne.class);
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private static String connectionUrl;

	public static void main(String[] args) {

		// Get url Connection
//		connectionUrl = "jdbc:hive2://localhost:10000/default;ssl=false";
		connectionUrl = args[0];

		// Init Connection
		Connection con = null;

		// ==== Select data from Hive Table
		String sqlStatementDrop = "DROP TABLE IF EXISTS helloworld";
		String sqlStatementCreate = "CREATE TABLE helloworld (message String) STORED AS PARQUET";
		String sqlStatementInsert = "INSERT INTO helloworld VALUES (\"helloworld\")";
		String sqlStatementSelect = "SELECT * from helloworld";
		String customerSelect = "SELECT * from customer";
		try {
			// Set JDBC Hive Driver
			Class.forName(JDBC_DRIVER_NAME);
			// Connect to Hive
			con = DriverManager.getConnection(connectionUrl, "", "");
			// Init Statement
			Statement stmt = con.createStatement();
			// Execute DROP TABLE Query
			stmt.execute(sqlStatementDrop);
			logger.info("Drop Hive table : OK");
			// Execute CREATE Query
			stmt.execute(sqlStatementCreate);
			logger.info("Create Hive table : OK");
			// Execute INSERT Query
			stmt.execute(sqlStatementInsert);
			logger.info("Insert into Hive table : OK");
			// Execute SELECT Query
			ResultSet rs = stmt.executeQuery(sqlStatementSelect);
			while (rs.next()) {
				logger.info(rs.getString(1));
			}
			logger.info("Select from Hive table : OK");
			
			rs = stmt.executeQuery(customerSelect);
			while (rs.next()) {
				logger.info(rs.getString(1));
			}
			logger.info("Select from Hive table : OK");

		} catch (Exception e) {
			logger.error("Exception = {} ",e.getMessage());
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				logger.error("Exception = {} ",e.getMessage());
			}
		}
	}
}
