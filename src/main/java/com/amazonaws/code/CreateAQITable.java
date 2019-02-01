package com.amazonaws.code;

import java.text.SimpleDateFormat;
import java.util.ArrayList;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class CreateAQITable {

	@SuppressWarnings("deprecation")
	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient());

	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	public static void main(String[] args) throws Exception {
		DateTime dt = new DateTime();
		DateTime dt8 = new DateTime(dt, -8);
		String TableName8 = "PM2.5_" + dt8.getEightDigitDate() + "";
		String TableName = "PM2.5_" + dt.getEightDigitDate() + "";
		try {

			deleteTable(TableName8);

			// Parameter1: table name // Parameter2: reads per second //
			// Parameter3: writes per second // Parameter4/5: partition key and data type
			// Parameter6/7: sort key and data type (if applicable)

			createTable(TableName, 10L, 5L, "location", "S");

		} catch (Exception e) {
			System.err.println("Program failed:");
			System.err.println(e.getMessage());
		}
		System.out.println("Success.");
	}

	private static void deleteTable(String tableName) {
		Table table = dynamoDB.getTable(tableName);
		try {
			System.out.println("Issuing DeleteTable request for " + tableName);
			table.delete();
			System.out.println("Waiting for " + tableName + " to be deleted...this may take a while...");
			table.waitForDelete();

		} catch (Exception e) {
			System.err.println("DeleteTable request failed for " + tableName);
			System.err.println(e.getMessage());
		}
	}

	private static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
			String partitionKeyName, String partitionKeyType) {

		createTable(tableName, readCapacityUnits, writeCapacityUnits, partitionKeyName, partitionKeyType, null, null);
	}

	private static void createTable(String tableName, long readCapacityUnits, long writeCapacityUnits,
			String partitionKeyName, String partitionKeyType, String sortKeyName1, String sortKeyType1) {

		try {

			ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
			keySchema.add(new KeySchemaElement().withAttributeName(partitionKeyName).withKeyType(KeyType.HASH)); // Partition
																													// key

			ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(
					new AttributeDefinition().withAttributeName(partitionKeyName).withAttributeType(partitionKeyType));

			if (sortKeyName1 != null) {
				keySchema.add(new KeySchemaElement().withAttributeName(sortKeyName1).withKeyType(KeyType.RANGE)); // Sort
																													// key
				attributeDefinitions
						.add(new AttributeDefinition().withAttributeName(sortKeyName1).withAttributeType(sortKeyType1));
			}

			CreateTableRequest request = new CreateTableRequest().withTableName(tableName).withKeySchema(keySchema)
					.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits)
							.withWriteCapacityUnits(writeCapacityUnits));

			request.setAttributeDefinitions(attributeDefinitions);

			System.out.println("Issuing CreateTable request for " + tableName);
			Table table = dynamoDB.createTable(request);
			System.out.println("Waiting for " + tableName + " to be created...this may take a while...");
			table.waitForActive();

		} catch (Exception e) {
			System.err.println("CreateTable request failed for " + tableName);
			System.err.println(e.getMessage());
		}
	}

}