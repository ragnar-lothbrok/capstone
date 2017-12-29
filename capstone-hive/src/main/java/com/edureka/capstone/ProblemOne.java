package com.edureka.capstone;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProblemOne {

	private static final Logger logger = LoggerFactory.getLogger(ProblemOne.class);
	private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	public static Properties loadProperties(String path) {
		Properties props = new Properties();
		try {
			FileInputStream fis = new FileInputStream(new File(path));
			props.load(fis);
			fis.close();
		} catch (Exception e) {
			logger.error("exception occured while loading file = {}", e);
		}
		return props;
	}

	static class YearMerchant implements Serializable {

		private static final long serialVersionUID = 1L;

		private String bank;
		private Double totalAmount;
		private Long totalTransaction;
		private Double totalTxAmountByTopMerchant;
		private Long totalOrderByTopMerchant;

		public String getBank() {
			return bank;
		}

		public void setBank(String bank) {
			this.bank = bank;
		}

		public Double getTotalAmount() {
			return totalAmount;
		}

		public void setTotalAmount(Double totalAmount) {
			this.totalAmount = totalAmount;
		}

		public Long getTotalTransaction() {
			return totalTransaction;
		}

		public void setTotalTransaction(Long totalTransaction) {
			this.totalTransaction = totalTransaction;
		}

		public Double getTotalTxAmountByTopMerchant() {
			return totalTxAmountByTopMerchant;
		}

		public void setTotalTxAmountByTopMerchant(Double totalTxAmountByTopMerchant) {
			this.totalTxAmountByTopMerchant = totalTxAmountByTopMerchant;
		}

		public Long getTotalOrderByTopMerchant() {
			return totalOrderByTopMerchant;
		}

		public void setTotalOrderByTopMerchant(Long totalOrderByTopMerchant) {
			this.totalOrderByTopMerchant = totalOrderByTopMerchant;
		}

		@Override
		public String toString() {
			return "YearMerchant [bank=" + bank + ", totalAmount=" + totalAmount + ", totalTransaction="
					+ totalTransaction + ", totalTxAmountByTopMerchant=" + totalTxAmountByTopMerchant
					+ ", totalTxAmount Percentage=" + (totalTxAmountByTopMerchant*100.0/totalAmount) + ", totalOrderByTopMerchant="
					+ totalOrderByTopMerchant + "]";
		}

	}

	public static void solveProblemOne(Properties props) {
		String distinctBanks = "select c.bank as bank ,sum(t.invoiceamount) as total ,count(*) as totalOrder from transaction t join card c where c.customerid=t.customerid and YEAR(from_utc_timestamp(tx_timestamp,'yyyy-mm-dd hh:mm:ss')) = "+Integer.parseInt(props.get("hive.problemone.year").toString())+" group by c.bank order by c.bank, total desc";
		Connection con = null;
		ResultSet rs = null;
		List<YearMerchant> list = new ArrayList<YearMerchant>();
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery(distinctBanks);
			while (rs.next()) {
				logger.info("Bank  = {} Total Amount = {} total order = {} ", rs.getString("bank"),
						rs.getDouble("total"), rs.getLong("totalOrder"));
				YearMerchant yearMerchant = new YearMerchant();
				yearMerchant.setBank(rs.getString("bank"));
				yearMerchant.setTotalAmount(rs.getDouble("total"));
				yearMerchant.setTotalTransaction(rs.getLong("totalOrder"));
				setTopMerchantData(props, yearMerchant.getBank(), yearMerchant);
				list.add(yearMerchant);
			}
		} catch (Exception e) {
			logger.error("Exception = {} ", e.getMessage());
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				logger.error("Exception while closing result set = {} ", e.getMessage());
			}
			try {
				con.close();
			} catch (Exception e) {
				logger.error("Exception while closing connection = {} ", e.getMessage());
			}
		}

		logger.info("Year wise merchant stats = {} ",list);
	}

	public static void setTopMerchantData(Properties props, String bank, YearMerchant yearMerchant) {
		bank = bank.replaceAll("\"", "");
		String query = "select sum(amount) as totalTxAmount,sum(orderCount) as totalOrderCount from (select t.merchantid as merchantid, sum(t.invoiceamount) as amount,count(*) as orderCount from transaction t join card c where c.customerid=t.customerid  and YEAR(from_utc_timestamp(tx_timestamp,'yyyy-mm-dd hh:mm:ss')) = "+Integer.parseInt(props.get("hive.problemone.year").toString())+" and c.bank= \"\\\""
				+ bank + "\\\"\" group by t.merchantid order by amount desc limit 10) t1";
		Connection con = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				yearMerchant.setTotalOrderByTopMerchant(rs.getLong("totalOrderCount"));
				yearMerchant.setTotalTxAmountByTopMerchant(rs.getDouble("totalTxAmount"));
			}
			logger.info("All done!!!");
		} catch (Exception e) {
			logger.error("Exception = {} ", e.getMessage());
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				logger.error("Exception while closing result set = {} ", e.getMessage());
			}
			try {
				con.close();
			} catch (Exception e) {
				logger.error("Exception while closing connection = {} ", e.getMessage());
			}
		}
	}

	public static void setCardTable(Properties props) {
		String sqlStatementDrop = "DROP TABLE IF EXISTS default.card";
		String sqlStatementCreate = "CREATE EXTERNAL TABLE IF NOT EXISTS card(cardid BIGINT,customerid BIGINT, bank STRING, type STRING, valid_month INT, valid_year INT, expirymonth INT, expiryyear INT,cardnum String, cardlimit double) COMMENT 'Card Details' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  tblproperties (\"skip.header.line.count\"=\"1\")";
		String loadData = "load data inpath '" + props.get("hive.cardpath").toString() + "' into table card";
		String countRows = "select * from card";

		Connection con = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();

			// Execute DROP TABLE Query
			Boolean dropped = stmt.execute(sqlStatementDrop);
			logger.info("Drop Hive table : OK = {} ", dropped);

			// Execute CREATE Query
			Boolean crreated = stmt.execute(sqlStatementCreate);
			logger.info("Create Hive table : OK = {} ", crreated);

			// Load data
			Boolean inserted = stmt.execute(loadData);
			logger.info("inserted Hive table : OK = {} ", inserted);

			rs = stmt.executeQuery(countRows);
			while (rs.next()) {
				logger.info("Total row count  = {} ", rs.getLong(1));
			}
			logger.info("All done!!!");
		} catch (Exception e) {
			logger.error("Exception = {} ", e.getMessage());
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				logger.error("Exception while closing result set = {} ", e.getMessage());
			}
			try {
				con.close();
			} catch (Exception e) {
				logger.error("Exception while closing connection = {} ", e.getMessage());
			}
		}
	}

	public static void setTransactionTable(Properties props) {
		String sqlStatementDrop = "DROP TABLE IF EXISTS default.transaction";
		String sqlStatementCreate = "CREATE EXTERNAL TABLE IF NOT EXISTS transaction(transactionid STRING, customerid BIGINT, merchantid BIGINT, tx_timestamp STRING,invoicenumber STRING,invoiceamount DOUBLE,segment STRING) COMMENT 'Transaction details' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' tblproperties (\"skip.header.line.count\"=\"1\")";
		String loadData = "load data inpath '" + props.get("hive.transactionpath").toString()
				+ "' into table transaction";
		String countRows = "select * from transaction";
		// String alterTxTable = "alter table transaction add columns( year
		// INTEGER,month INTEGER)";
		// String updateNewColumns = "update transaction set
		// year=YEAR(from_utc_timestamp(tx_timestamp,'yyyy-mm-dd hh:mm:ss')) ,
		// month=MONTH(from_utc_timestamp(tx_timestamp,'yyyy-mm-dd hh:mm:ss'))";

		Connection con = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();

			// Execute DROP TABLE Query
			Boolean dropped = stmt.execute(sqlStatementDrop);
			logger.info("Drop Hive table : OK = {} ", dropped);

			// Execute CREATE Query
			Boolean crreated = stmt.execute(sqlStatementCreate);
			logger.info("Create Hive table : OK = {} ", crreated);

			// Load data
			Boolean inserted = stmt.execute(loadData);
			logger.info("inserted Hive table : OK = {} ", inserted);

			rs = stmt.executeQuery(countRows);
			while (rs.next()) {
				logger.info("Total row count  = {} ", rs.getLong(1));
			}
			logger.info("All done!!!");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception = {} ", e.getMessage());
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				logger.error("Exception while closing result set = {} ", e.getMessage());
			}
			try {
				con.close();
			} catch (Exception e) {
				logger.error("Exception while closing connection = {} ", e.getMessage());
			}
		}
	}

	public static void setCustomerTable(Properties props) {
		String sqlStatementDrop = "DROP TABLE IF EXISTS default.customer";
		String sqlStatementCreate = "CREATE EXTERNAL TABLE IF NOT EXISTS customer(customerid BIGINT, name STRING, mobilenumber STRING, gender STRING, title STRING,martial_status STRING,birthdate STRING,email STRING,address STRING,state STRING,country STRING,pincode BIGINT,created STRING) COMMENT 'Customer Detail' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' tblproperties (\"skip.header.line.count\"=\"1\")";
		String loadData = "load data inpath '" + props.get("hive.customerpath").toString() + "' into table customer";
		String countRows = "select * from customer";

		Connection con = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();

			// Execute DROP TABLE Query
			Boolean dropped = stmt.execute(sqlStatementDrop);
			logger.info("Drop Hive table : OK = {} ", dropped);

			// Execute CREATE Query
			Boolean crreated = stmt.execute(sqlStatementCreate);
			logger.info("Create Hive table : OK = {} ", crreated);

			// Load data
			Boolean inserted = stmt.execute(loadData);
			logger.info("inserted Hive table : OK = {} ", inserted);

			rs = stmt.executeQuery(countRows);
			while (rs.next()) {
				logger.info("Total row count  = {} ", rs.getLong(1));
			}
			logger.info("All done!!!");
		} catch (Exception e) {
			logger.error("Exception = {} ", e.getMessage());
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				logger.error("Exception while closing result set = {} ", e.getMessage());
			}
			try {
				con.close();
			} catch (Exception e) {
				logger.error("Exception while closing connection = {} ", e.getMessage());
			}
		}
	}

	public static void main(String[] args) {

		// Loading properties
		// Properties props = loadProperties(args[0]);
		Properties props = loadProperties(
				"/Users/raghugupta/Documents/gitrepo/capstone/capstone-hive/src/main/resources/application.properties");
		if (Boolean.parseBoolean(props.get("hive.local.props").toString())) {
			System.setProperty("hadoop.home.dir", "/Users/raghugupta/Downloads/hadoop-2.9.0/");
			System.setProperty("hive.metastore.warehouse.dir", "hdfs://localhost:9000/");
			System.setProperty(HiveConf.ConfVars.SUBMITLOCALTASKVIACHILD.varname, "false");
			// System.setProperty("HADOOP_CONF_DIR","/Users/raghugupta/Downloads/hadoop-2.9.0/etc/hadoop/");
			System.setProperty("HADOOP_MAPRED_HOME", "/Users/raghugupta/Downloads/hadoop-2.9.0/");
			System.setProperty("mapreduce.framework.name", "local");
			System.setProperty("mapreduce.jobtracker.address", "localhost:9001");
		}
		// setCustomerTable(props);
		// setTransactionTable(props);
		// setCardTable(props);
		solveProblemOne(props);

		System.exit(1);
	}
}
