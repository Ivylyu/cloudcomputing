package com.amazonaws.code;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

public class TestScanDB {

	public static void main(String[] args) {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("location", new AttributeValue().withS("Anqing An Qing Da Xue"));

		GetItemRequest request = new GetItemRequest().withTableName("Site").withKey(key);

		try {
			GetItemResult result = client.getItem(request);
			if ((result != null) && (result.getItem() != null)) {
				AttributeValue lc = result.getItem().get("city");
				System.out.println("The location is in " + lc.getN());
			} else {
				System.out.println("No matching location was found");
			}
		} catch (Exception e) {
			System.err.println("Unable to retrieve data: ");
			System.err.println(e.getMessage());
		}
	}
}