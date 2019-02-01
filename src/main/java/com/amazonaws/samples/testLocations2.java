package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.code.DateTime;
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

public class testLocations2 {

	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new ProfileCredentialsProvider()));
	static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	public static void main(String[] args) throws Exception {

		// Table table = dynamoDB.getTable("Site");

		DateTime dt = new DateTime();

		System.out.println(dt.getEightDigitDate());
		String TableName = "PM2.5_" + dt.getEightDigitDate() + "";
		System.out.println(TableName);

		// InputStreamReader reader = new InputStreamReader(new
		// File("test_create.json"), "UTF-8");
		FileInputStream fis = new FileInputStream("test.json");
		InputStreamReader reader = new InputStreamReader(fis, "UTF-8");

		JsonParser parser = new JsonFactory().createParser(reader);

		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();

		while (iter.hasNext()) {
			JsonNode results = (JsonNode) iter.next();
			String location = results.path("location").asText();
			System.out.println(location);
			// String city = results.path("city").asText();
			// String latitude = results.path("coordinates").at("/latitude").asText();
			// String longitude = results.path("coordinates").at("/longitude").asText();
			//
			// System.out.println("location: " + location + ", city: " + city + ",
			// latitude:" + latitude + ", longitude :"
			// + longitude);
			//
			// // try {
			// // table.putItem(new Item().withPrimaryKey("siteID",
			// // siteID).withNumber("latitude", latitude)
			// // .withNumber("longtitude", longitude));
			// // System.out.println("PutItem succeeded: " + siteID + " " + longitude + " "
			// +
			// // latitude);
			// //
			// // } catch (Exception e) {
			// // System.err.println("Unable to add site: " + siteID + " " + longitude + " "
			// +
			// // latitude);
			// // System.err.println(e.getMessage());
			// // break;
			// // }
			// }
			parser.close();
		}
	}
}