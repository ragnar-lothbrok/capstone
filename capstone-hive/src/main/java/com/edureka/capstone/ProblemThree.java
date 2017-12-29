package com.edureka.capstone;

import java.io.File;
import java.io.FileInputStream;
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


public class ProblemThree {

	private static final Logger logger = LoggerFactory.getLogger(ProblemTwo.class);
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

	static class MerchantSegmentMonthly {
		private Long merchant;
		private String segment;
		private Integer month;
		private double amount;
		private Long orderCount;
		

		public Long getOrderCount() {
			return orderCount;
		}

		public void setOrderCount(Long orderCount) {
			this.orderCount = orderCount;
		}

		public double getAmount() {
			return amount;
		}

		public void setAmount(double amount) {
			this.amount = amount;
		}

		public Long getMerchant() {
			return merchant;
		}

		public void setMerchant(Long merchant) {
			this.merchant = merchant;
		}

		public String getSegment() {
			return segment;
		}

		public void setSegment(String segment) {
			this.segment = segment;
		}

		public Integer getMonth() {
			return month;
		}

		public void setMonth(Integer month) {
			this.month = month;
		}

		@Override
		public String toString() {
			return "MerchantSegmentMonthly [merchant=" + merchant + ", segment=" + segment + ", month=" + month
					+ ", amount=" + amount + ", orderCount=" + orderCount + "]";
		}

	}

	public static void solveProblem(Properties props) {
		List<Long> merchantIds = ProblemTwo.topMerchants(props);
		String merchantIdsStr = "";
		if (merchantIds != null) {
			for (Long merchant : merchantIds) {
				merchantIdsStr += merchant + ",";
			}
			merchantIdsStr = merchantIdsStr.substring(0, merchantIdsStr.length() - 1);
		}

		String query = "select t.merchantid as merchantid,t.segment as segment, MONTH(from_utc_timestamp(t.tx_timestamp,'yyyy-mm-dd hh:mm:ss')) as month, sum(t.invoiceamount) as amount, count(*) as orderCount from transaction t where YEAR(from_utc_timestamp(tx_timestamp,'yyyy-mm-dd hh:mm:ss'))= "
				+ Integer.parseInt(props.get("hive.problemone.year").toString())
				+ " and t.merchantid in("+merchantIdsStr+") group by t.merchantid,t.segment,MONTH(from_utc_timestamp(t.tx_timestamp,'yyyy-mm-dd hh:mm:ss'))";
		
		Connection con = null;
		ResultSet rs = null;
		List<MerchantSegmentMonthly> list = new ArrayList<MerchantSegmentMonthly>();
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				MerchantSegmentMonthly merchantSegmentMonthly = new MerchantSegmentMonthly();
				merchantSegmentMonthly.setMerchant(rs.getLong("merchantid"));
				merchantSegmentMonthly.setSegment(rs.getString("segment"));
				merchantSegmentMonthly.setMonth(rs.getInt("month"));
				merchantSegmentMonthly.setAmount(rs.getDouble("amount"));
				merchantSegmentMonthly.setOrderCount(rs.getLong("orderCount"));
				list.add(merchantSegmentMonthly);
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

		logger.info("Monthly wise merchant segment stats = {} ", list);
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
		// ProblemOne.setTransactionTable(props);
		solveProblem(props);
		System.exit(1);
	}
}
