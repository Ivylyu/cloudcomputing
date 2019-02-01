package com.amazonaws.code;

import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PM10LoadData {

	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient());

	public static void main(String[] args) throws Exception {

		DateTime dt = new DateTime();
		String TableName = "PM10_" + dt.getEightDigitDate() + "";

		System.out.println("Openning " + TableName + " table");
		Table table = dynamoDB.getTable(TableName);

		URL url = new URL("https://api.openaq.org/v1/latest?country=CN&&parameter=pm10&&limit=1500");
		InputStreamReader reader = new InputStreamReader(url.openStream(), "UTF-8");
		JsonParser parser = new JsonFactory().createParser(reader);

		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> iter = rootNode.iterator();

		JsonNode currentNode;

		currentNode = (JsonNode) iter.next();
		currentNode = (JsonNode) iter.next();
		Iterator<JsonNode> iter2 = currentNode.iterator();

		while (iter2.hasNext()) {
			currentNode = (JsonNode) iter2.next();

			String location = currentNode.path("location").asText();
			String time = DateTime.getCurrentTime();
			JsonNode currentNode2 = currentNode.findParent("value");
			ArrayList<Integer> pm10 = new ArrayList<>();
			pm10.add(currentNode2.path("value").asInt());
			String hour = String.valueOf(new Date().getHours() + 1);
			final Map<String, Object> infoMap = new HashMap<String, Object>();
			infoMap.put(hour, pm10.get(0));

			if (table.getItem("location", location) != null) {
				UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("location", location)
						.withUpdateExpression("set Info.#hour=:val").withNameMap(new NameMap().with("#hour", hour))
						.withValueMap(new ValueMap().withNumber(":val", pm10.get(0)))
						.withReturnValues(ReturnValue.UPDATED_NEW);

				try {
					UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
				} catch (Exception e) {
					System.err.println("Unable to update item: " + location + " ");
					System.err.println(e.getMessage());
				}
			} else {
				try {
					table.putItem(new Item().withPrimaryKey("location", location).withString("date", time)
							.withMap("Info", infoMap));
				} catch (Exception e) {
					System.err.println("Unable to add site: " + location + " ");
					System.err.println(e.getMessage());
					break;
				}

			}

		}
		reader.close();
		parser.close();
	}
}