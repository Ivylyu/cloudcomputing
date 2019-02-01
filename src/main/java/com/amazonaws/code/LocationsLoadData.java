package com.amazonaws.code;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
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

public class LocationsLoadData {

	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));
	// static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	// .withEndpointConfiguration(new
	// AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west2"))
	// .build();
	//
	// static DynamoDB dynamoDB = new DynamoDB(client);

	public static void main(String[] args) throws Exception {

		Table table = dynamoDB.getTable("Site");

		URL url = new URL("https://api.openaq.org/v1/locations?country=CN&&limit=1500");
		InputStreamReader reader = new InputStreamReader(url.openStream(), "UTF-8");

		JsonParser parser = new JsonFactory().createParser(reader);

		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();

		JsonNode currentNode;

		currentNode = (JsonNode) iter.next();
		currentNode = (JsonNode) iter.next();

		Iterator<JsonNode> iter2 = currentNode.iterator();

		while (iter2.hasNext()) {
			JsonNode results = (JsonNode) iter2.next();
			String location = results.path("location").asText();
			String city = results.path("city").asText();
			double latitude = results.path("coordinates").at("/latitude").asDouble();
			double longitude = results.path("coordinates").at("/longitude").asDouble();

			try {
				table.putItem(new Item().withPrimaryKey("location", location).withString("city", city)
						.withNumber("latitude", latitude).withNumber("longtitude", longitude));
				System.out.println("PutItem succeeded: " + location + " " + longitude + " " + latitude);

			} catch (Exception e) {
				System.err.println("Unable to add site: " + location + " " + longitude + " " + latitude);
				System.err.println(e.getMessage());
				break;
			}
		}
		reader.close();
		parser.close();
	}
}