package com.edureka.kafka.utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.validation.ValidationProviderResolver;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.edureka.kafka.api.EventProducerApi;
import com.edureka.kafka.service.AvroSchemaDefinitionLoader;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class FileUtility {

	private static final Logger logger = LoggerFactory.getLogger(FileUtility.class);

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
	private static SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static void readFile(String filePath, String schemaFilePath, String topic,
			EventProducerApi eventProducerApi) {

		String schemaFileContent = AvroSchemaDefinitionLoader.fromFile(schemaFilePath).get();
		BufferedReader br = null;
		FileReader fr = null;
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
				sCurrentLine = sCurrentLine.replace("\"", "");
				String split[] = sCurrentLine.split(",");

				if ("transaction".equalsIgnoreCase(schema.getName())) {
					avroRecord.put("txId", Long.parseLong(split[0]));
					avroRecord.put("customerId", Long.parseLong(split[1]));
					avroRecord.put("merchantId", Long.parseLong(split[2]));
					avroRecord.put("status", "NA");
					try {
						avroRecord.put("timestamp", sdf.parse(split[3]).getTime());
					} catch (ParseException e) {
						logger.error("Exception occured while parsing date = {} e = {}  ", split[3], e);
						try {
							avroRecord.put("timestamp", sdf3.parse(split[3]).getTime());
						} catch (ParseException e1) {
							logger.error("Exception occured while parsing date = {} e = {}  ", split[3], e);
						}
					}
					avroRecord.put("invoiceNum", split[4].trim());
					avroRecord.put("invoiceAmount", Float.parseFloat(split[5]));
					avroRecord.put("segment", split[6].trim());
				} else if ("card".equalsIgnoreCase(schema.getName())) {
					avroRecord.put("cardId", Long.parseLong(split[0]));
					avroRecord.put("customerId", Long.parseLong(split[1]));
					avroRecord.put("bank", split[2].trim());
					avroRecord.put("type", split[3].trim());
					avroRecord.put("valid_month", Integer.parseInt(split[4].trim()));
					avroRecord.put("valid_year", Integer.parseInt(split[5].trim()));
					avroRecord.put("expiryMonth", Integer.parseInt(split[6].trim()));
					avroRecord.put("expiryYear", Integer.parseInt(split[7].trim()));
					avroRecord.put("cardNum", split[8].trim());
					avroRecord.put("cardLimit", Float.parseFloat(split[9].trim()));
				} else if ("merchant".equalsIgnoreCase(schema.getName())) {
					System.out.println(sCurrentLine);
					try {
						avroRecord.put("merchantId", Long.parseLong(split[0]));
						avroRecord.put("taxRegNum", split[1].trim());
						avroRecord.put("merchantName", split[2]);
						avroRecord.put("mobileNumber", Long.parseLong(split[3].trim()));
						try {
							avroRecord.put("startDate", sdf.parse(split[4]).getTime());
						} catch (ParseException e) {
							logger.error("Exception occured while parsing date = {} e = {}  ", split[4], e);
						}

						avroRecord.put("email", split[5].trim());
						avroRecord.put("address", split[6].trim());
						avroRecord.put("state", split[7].trim());
						avroRecord.put("country", split[8].trim());
						avroRecord.put("pincode", Integer.parseInt(split[9].trim()));
						avroRecord.put("description", split[10].trim());
						avroRecord.put("longlat", split[11].trim());
					} catch (Exception e) {
						logger.error("exception occured e {} ", e);
					}
				} else if ("customer".equalsIgnoreCase(schema.getName())) {
					avroRecord.put("customerId", Long.parseLong(split[0]));
					avroRecord.put("customerName", split[1]);
					avroRecord.put("mobileNumber", Long.parseLong(split[2].trim()));
					avroRecord.put("gender", split[3]);
					avroRecord.put("title", split[4]);
					avroRecord.put("martial_status", split[5]);
					try {
						avroRecord.put("bithDate", sdf2.parse(split[6]).getTime());
					} catch (ParseException e) {
						logger.error("Exception occured while parsing date = {} e = {}  ", split[6], e);
					}

					avroRecord.put("email", split[7].trim());
					avroRecord.put("address", split[8].trim());
					avroRecord.put("state", split[9].trim());
					avroRecord.put("country", split[10].trim());
					avroRecord.put("pincode", Integer.parseInt(split[11].trim()));
					try {
						avroRecord.put("created", sdf.parse(split[12]).getTime());
					} catch (ParseException e) {
						logger.error("Exception occured while parsing date = {} e = {}  ", split[4], e);
					}
				}
				eventProducerApi.dispatch(recordInjection.apply(avroRecord), topic);
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
	}
}
