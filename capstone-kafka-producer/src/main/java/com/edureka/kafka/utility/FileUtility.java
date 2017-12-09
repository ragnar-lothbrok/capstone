package com.edureka.kafka.utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.kafka.service.AvroSchemaDefinitionLoader;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class FileUtility {

	private static final Logger logger = LoggerFactory.getLogger(FileUtility.class);

	private static SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yy");
	private static SimpleDateFormat sdf1 = new SimpleDateFormat("mm/dd/yyyy");

	public static List<String> readFile(String filePath, String schemaFilePath) {

		String schemaFileContent = AvroSchemaDefinitionLoader.fromFile(schemaFilePath).get();
		BufferedReader br = null;
		FileReader fr = null;
		List<String> eventsList = new ArrayList<String>();
		try {
			Schema.Parser parser = new Schema.Parser();
			Schema schema = parser.parse(schemaFileContent);

			Injection<GenericRecord, String> recordInjection = GenericAvroCodecs.toJson(schema);

			fr = new FileReader(filePath);
			br = new BufferedReader(fr);

			String sCurrentLine;
			sCurrentLine = br.readLine();
			while ((sCurrentLine = br.readLine()) != null) {
				GenericData.Record avroRecord = new GenericData.Record(schema);
				String split[] = sCurrentLine.split(",");

				if ("transaction".equalsIgnoreCase(schema.getName())) {
					avroRecord.put("txId", Long.parseLong(split[0]));
					avroRecord.put("customerId", Long.parseLong(split[1]));
					avroRecord.put("merchantId", Long.parseLong(split[2]));
					avroRecord.put("status", split[3].trim());
					try {
						avroRecord.put("timestamp", sdf.parse(split[4]).getTime());
					} catch (ParseException e) {
						logger.error("Exception occured while parsing date = {} e = {}  ", split[4], e);
					}
					avroRecord.put("invoiceNum", split[5].trim());
					avroRecord.put("invoiceAmount", Float.parseFloat(split[6]));
				} else if ("card".equalsIgnoreCase(schema.getName())) {
					avroRecord.put("cardId", Long.parseLong(split[0]));
					avroRecord.put("customerId", Long.parseLong(split[1]));
					avroRecord.put("bank", split[2].trim());
					avroRecord.put("type", split[3].trim());
					avroRecord.put("nameOnCard", split[4].trim());
					avroRecord.put("expiryMonth", Integer.parseInt(split[5].trim()));
					avroRecord.put("expiryYear", Integer.parseInt(split[6].trim()));
					avroRecord.put("cardNum", split[7].trim());
					avroRecord.put("cardLimit", Float.parseFloat(split[8].trim()));
				} else if ("merchant".equalsIgnoreCase(schema.getName())) {
					System.out.println(sCurrentLine);
					try {
						avroRecord.put("merchantId", Long.parseLong(split[0]));
						avroRecord.put("merchantName", split[1]);
						avroRecord.put("mobileNumber", Long.parseLong(split[2].trim()));
						try {
							avroRecord.put("startDate", sdf1.parse(split[3]).getTime());
						} catch (ParseException e) {
							logger.error("Exception occured while parsing date = {} e = {}  ", split[4], e);
						}

						avroRecord.put("email", split[4].trim());
						avroRecord.put("address", split[5].trim());
						avroRecord.put("state", split[6].trim());
						avroRecord.put("country", split[7].trim());
						avroRecord.put("pincode", Integer.parseInt(split[8].trim()));
						avroRecord.put("merchantType", Integer.parseInt(split[9].trim()));
						avroRecord.put("segment", split[10].trim());
						avroRecord.put("taxRegNum", split[11].trim());
						if(split.length == 13)
							avroRecord.put("description", split[12].trim());
						else{
							avroRecord.put("description", "");
						}
					} catch (Exception e) {
						System.out.println();
					}
				} else if ("customer".equalsIgnoreCase(schema.getName())) {
					avroRecord.put("customerId", Long.parseLong(split[0]));
					avroRecord.put("customerName", split[1]);
					avroRecord.put("mobileNumber", Long.parseLong(split[2].trim()));
					avroRecord.put("gender", split[3]);
					try {
						avroRecord.put("bithDate", sdf.parse(split[4]).getTime());
					} catch (ParseException e) {
						logger.error("Exception occured while parsing date = {} e = {}  ", split[4], e);
					}

					avroRecord.put("email", split[5].trim());
					avroRecord.put("address", split[6].trim());
					avroRecord.put("state", split[7].trim());
					avroRecord.put("country", split[8].trim());
					avroRecord.put("pincode", Integer.parseInt(split[9].trim()));
					avroRecord.put("userType", Integer.parseInt(split[10].trim()));
					avroRecord.put("vehicleType", split[11].trim());
					try {
						avroRecord.put("firstVisit", sdf1.parse(split[12]).getTime());
					} catch (ParseException e) {
						logger.error("Exception occured while parsing date = {} e = {}  ", split[4], e);
					}
				}

				eventsList.add(recordInjection.apply(avroRecord));
			}
		} catch (IOException e) {
			logger.error("Exception occured while reading file = {} ", e);
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				logger.error("Exception occured while releasing file resources = {} ", ex);
			}
		}
		return eventsList;
	}
}
