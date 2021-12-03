package br.com.senior.seniorx.integration.state.ddb;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.spi.PropertiesComponent;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;

import br.com.senior.seniorx.integration.state.IntegrationState;
import br.com.senior.seniorx.integration.state.IntegrationStateException;

public class CamelDDBIntegrationState implements IntegrationState {

    private static final String SELECTOR_HEADER = "selector";
    private static final String SELECTOR_HEADER_NOT_FOUND = "Selector header not found";
    private static final String INTEGRATION_ID_HEADER = "integration_id";
    private static final String TENANT_FIELD = "Tenant";
    private static final String ID_FIELD = "Id";
    private static final String STATE_FIELD = "State";

    private final Exchange exchange;
    private final AmazonDynamoDB ddb;
    private final String table;
    private String integrationName;

    public CamelDDBIntegrationState(Exchange exchange, String integrationName) {
        this.exchange = exchange;
        this.integrationName = integrationName;
        this.ddb = connectToDynamoDB();
        this.table = getParametersTable();
    }

    @Override
    public void put() {
        Object selector = exchange.getIn().getHeader(SELECTOR_HEADER);
        if (selector == null) {
            throw new IntegrationStateException(SELECTOR_HEADER_NOT_FOUND);
        }
        String tenant = selector.toString();

        Message message = exchange.getMessage();
        Object currentId = message.getHeader(INTEGRATION_ID_HEADER);
        String id;
        if (currentId != null) {
            id = currentId.toString();
        } else {
            id = UUID.randomUUID().toString();
            message.setHeader(INTEGRATION_ID_HEADER, id);
        }

        PutItemRequest request = new PutItemRequest().withTableName(table);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TENANT_FIELD, new AttributeValue(tenant));
        item.put(ID_FIELD, new AttributeValue(integrationName + '-' + id));
        item.put(STATE_FIELD, new AttributeValue(message.getBody().toString()));
        request.withItem(item);
        ddb.putItem(request);
    }

    @Override
    public String get() {
        Object selector = exchange.getIn().getHeader(SELECTOR_HEADER);
        if (selector == null) {
            throw new IntegrationStateException(SELECTOR_HEADER_NOT_FOUND);
        }
        String tenant = selector.toString();

        Message message = exchange.getMessage();
        Object currentId = message.getHeader(INTEGRATION_ID_HEADER);
        if (currentId == null) {
            return null;
        }
        String id = currentId.toString();

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(TENANT_FIELD, new AttributeValue(tenant));
        key.put(ID_FIELD, new AttributeValue(integrationName + '-' + id));

        GetItemRequest request = new GetItemRequest().withKey(key).withTableName(table);
        Map<String, AttributeValue> item = ddb.getItem(request).getItem();
        if (item == null) {
            return null;
        }
        AttributeValue state = item.get(STATE_FIELD);
        if (state == null) {
            return null;
        }
        return state.getS();
    }

    @Override
    public void delete() {
        Object selector = exchange.getIn().getHeader(SELECTOR_HEADER);
        if (selector == null) {
            throw new IntegrationStateException(SELECTOR_HEADER_NOT_FOUND);
        }
        String tenant = selector.toString();

        Message message = exchange.getMessage();
        Object currentId = message.getHeader(INTEGRATION_ID_HEADER);
        if (currentId == null) {
            return;
        }
        String id = currentId.toString();

        Map<String, AttributeValue> key = new HashMap<>();
        key.put(TENANT_FIELD, new AttributeValue(tenant));
        key.put(ID_FIELD, new AttributeValue(integrationName + '-' + id));

        DeleteItemRequest request = new DeleteItemRequest().withKey(key).withTableName(table);
        ddb.deleteItem(request);
    }

    private AmazonDynamoDB connectToDynamoDB() {
        PropertiesComponent properties = exchange.getContext().getPropertiesComponent();
        String awsAccessKey = properties.resolveProperty("integration.aws.access.key").orElse(null);
        String awsSecretKey = properties.resolveProperty("integration.aws.secret.key").orElse(null);
        String region = properties.resolveProperty("integration.ddb.region").orElse("sa-east-1");
        return AmazonDynamoDBClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey))).withRegion(region).build();
    }

    private String getParametersTable() {
        return exchange.getProperty("integration.ddb.state.table", "IntegrationState", String.class);
    }

}
