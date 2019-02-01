package com.amazonaws.code;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AQILoadData {

	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));

	public static void main(String[] args) throws Exception {

		Table table = dynamoDB.getTable("AQI_20190124");

		JsonParser parser = new JsonFactory().createParser(new File("AQI_20190124.json"));

		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();

		ObjectNode currentNode;

		while (iter.hasNext()) {
			currentNode = (ObjectNode) iter.next();

			String siteID = currentNode.path("siteName").asText();

			ArrayList<Double> hour = new ArrayList<>();

			for (int i = 0; i < 24; i++) {
				String a = "" + i + "";
				hour.add(currentNode.path(a).asDouble());
			}

			try {
				table.putItem(new Item().withPrimaryKey("siteID", siteID).withNumber("0", hour.get(0))
						.withNumber("1", hour.get(1)).withNumber("2", hour.get(2)).withNumber("3", hour.get(3))
						.withNumber("4", hour.get(4)).withNumber("5", hour.get(5)).withNumber("6", hour.get(6))
						.withNumber("7", hour.get(7)).withNumber("8", hour.get(8)).withNumber("9", hour.get(9))
						.withNumber("10", hour.get(10)).withNumber("11", hour.get(11)).withNumber("12", hour.get(12))
						.withNumber("13", hour.get(13)).withNumber("14", hour.get(14)).withNumber("15", hour.get(15))
						.withNumber("16", hour.get(16)).withNumber("17", hour.get(17)).withNumber("18", hour.get(18))
						.withNumber("19", hour.get(19)).withNumber("20", hour.get(20)).withNumber("21", hour.get(21))
						.withNumber("22", hour.get(22)).withNumber("23", hour.get(23)));
				System.out.println("PutItem succeeded: " + siteID + " ");
				for (int i = 0; i < 24; i++) {
					System.out.print(hour.get(i) + ", ");
				}
				System.out.println();

			} catch (Exception e) {
				System.err.println("Unable to add site: " + siteID + " ");
				for (int i = 0; i < 24; i++) {
					System.out.print("Hour : " + hour.get(i));
				}
				System.out.println();
				System.err.println(e.getMessage());
				break;
			}
		}
		parser.close();
	}
}