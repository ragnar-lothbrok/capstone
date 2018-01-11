package com.edureka.capstone.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Resources;

public class AvroSchemaDefinitionLoader {

	private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaDefinitionLoader.class);

	public static Optional<String> fromFile(String fileName) {
		try {
			return Optional.of(Resources.toString(Resources.getResource(fileName), Charsets.UTF_8));
		} catch (Exception e) {
			LOGGER.error("Error loading schema file " + fileName, e);
			return Optional.absent();
		}
	}
}
