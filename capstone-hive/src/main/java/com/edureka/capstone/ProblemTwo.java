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

public class ProblemTwo {

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

	static class QuaterlyMerchant implements Serializable {

		private static final long serialVersionUID = 1L;

		private Long merchantId;
		private String quarter;
		private Double txAmount;
		private Long orderCount;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((merchantId == null) ? 0 : merchantId.hashCode());
			result = prime * result + ((quarter == null) ? 0 : quarter.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			QuaterlyMerchant other = (QuaterlyMerchant) obj;
			if (merchantId == null) {
				if (other.merchantId != null)
					return false;
			} else if (!merchantId.equals(other.merchantId))
				return false;
			if (quarter == null) {
				if (other.quarter != null)
					return false;
			} else if (!quarter.equals(other.quarter))
				return false;
			return true;
		}

		public Long getOrderCount() {
			return orderCount;
		}

		public void setOrderCount(Long orderCount) {
			this.orderCount = orderCount;
		}

		public Long getMerchantId() {
			return merchantId;
		}

		public void setMerchantId(Long merchantId) {
			this.merchantId = merchantId;
		}

		public String getQuarter() {
			return quarter;
		}

		public void setQuarter(String quarter) {
			this.quarter = quarter;
		}

		public Double getTxAmount() {
			return txAmount;
		}

		public void setTxAmount(Double txAmount) {
			this.txAmount = txAmount;
		}

		@Override
		public String toString() {
			return "QuaterlyMerchant [merchantId=" + merchantId + ", quarter=" + quarter + ", txAmount=" + txAmount
					+ ", orderCount=" + orderCount + "]";
		}
	}
	
	
	public static List<Long> topMerchants(Properties props){
		List<Long> list = new ArrayList<>();
		String query = "select t.merchantid as merchantid, sum(t.invoiceamount) as amount from transaction t where YEAR(from_utc_timestamp(tx_timestamp,'yyyy-mm-dd hh:mm:ss'))= "
				+ Integer.parseInt(props.get("hive.problemone.year").toString())
				+ " group by t.merchantid order by amount desc limit 5";

		Connection con = null;
		ResultSet rs = null;
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				list.add(rs.getLong("merchantid"));
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

		logger.info("Quaterly wise merchant stats = {} ", list);
		return list;
	}

	public static List<QuaterlyMerchant> solveProblemTwo(Properties props) {
		List<Long> merchantIds = topMerchants(props);
		String merchantIdsStr = "";
		if (merchantIds != null) {
			for (Long merchant : merchantIds) {
				merchantIdsStr += merchant + ",";
			}
			merchantIdsStr = merchantIdsStr.substring(0, merchantIdsStr.length() - 1);
		}

		String query = "select t.merchantid as merchantid, MONTH(from_utc_timestamp(t.tx_timestamp,'yyyy-mm-dd hh:mm:ss')) as month, sum(t.invoiceamount) as amount, count(*) as orderCount from transaction t where YEAR(from_utc_timestamp(tx_timestamp,'yyyy-mm-dd hh:mm:ss'))= "
				+ Integer.parseInt(props.get("hive.problemone.year").toString())
				+ " and t.merchantid in("+merchantIdsStr+") group by t.merchantid,MONTH(from_utc_timestamp(t.tx_timestamp,'yyyy-mm-dd hh:mm:ss')) order by amount desc";

		Connection con = null;
		ResultSet rs = null;
		List<QuaterlyMerchant> list = new ArrayList<QuaterlyMerchant>();
		try {
			Class.forName(JDBC_DRIVER_NAME);
			con = DriverManager.getConnection(props.get("hive.url").toString(), props.get("hive.username").toString(),
					props.get("hive.password").toString());
			Statement stmt = con.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				QuaterlyMerchant quaterly = new QuaterlyMerchant();
				quaterly.setMerchantId(rs.getLong("merchantid"));
				quaterly.setQuarter("Quarter " + ((rs.getInt("month")-1)/4)+1);
				quaterly.setTxAmount(rs.getDouble("amount"));
				quaterly.setOrderCount(rs.getLong("orderCount"));
				
				int index = list.indexOf(quaterly);
				if(index != -1) {
					QuaterlyMerchant existingMerchant = list.get(index);
					existingMerchant.setOrderCount(existingMerchant.getOrderCount() + quaterly.getOrderCount());
					existingMerchant.setTxAmount(existingMerchant.getTxAmount() + quaterly.getTxAmount());
				}else {
					list.add(quaterly);
				}
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

		logger.info("Quaterly wise merchant stats = {} ", list);
		return list;
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
		solveProblemTwo(props);

		System.exit(1);
	}
}
