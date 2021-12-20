package br.com.senior.seniorx.integration.state.ddb;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.spi.PropertiesComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.senior.seniorx.integration.state.IntegrationState;
import br.com.senior.seniorx.integration.state.IntegrationStateException;

public class CamelDDBIntegrationState implements IntegrationState {

    private static final Logger LOGGER = LoggerFactory.getLogger(CamelDDBIntegrationState.class);

    private static final String SELECTOR_HEADER = "selector";
    private static final String SELECTOR_HEADER_NOT_FOUND = "Selector header not found";
    private static final String CONTEXT_HEADER_NOT_FOUND = "Context header not found";
    private static final String STATE_NOT_FOUND = "State not found";
    private static final String STATE_PAYLOAD_NOT_FOUND = "State payload not found";
    private static final String CONTEXT = "context";
    private static final String TENANT_FIELD = "Tenant";
    private static final String ID_FIELD = "Id";
    private static final String DATA_FIELD = "Data";
    private static final String STATE_FIELD = "State";
    private static final String STATE_MESSAGE_FIELD = "StateMessage";

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
    public void put(String state, String stateMessage) {
        String tenant = getTenant();

        Message message = exchange.getMessage();
        Object currentContext = message.getHeader(CONTEXT);
        String context;
        if (currentContext != null) {
            context = currentContext.toString();
        } else {
            context = UUID.randomUUID().toString();
            message.setHeader(CONTEXT, context);
        }

        PutItemRequest request = new PutItemRequest().withTableName(table);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(TENANT_FIELD, new AttributeValue(tenant));
        String key = stateKey(context);
        item.put(ID_FIELD, new AttributeValue(key));
        ObjectMapper mapper = new ObjectMapper();
        String json;
        try {
            json = mapper.writeValueAsString(message.getBody());
            item.put(DATA_FIELD, new AttributeValue(json));
        } catch (JsonProcessingException e) {
            throw new IntegrationStateException(e);
        }
        if (state != null) {
            item.put(STATE_FIELD, new AttributeValue(state));
            if (stateMessage != null) {
                item.put(STATE_MESSAGE_FIELD, new AttributeValue(stateMessage));
            }
        }
        request.withItem(item);
        LOGGER.info("Saving state {} with message {} and body {} for context {} for tenant {}.", state, stateMessage, json, key, tenant);
        ddb.putItem(request);
    }

    @Override
    public <T> T get(Class<T> dataClass) {
        String tenant = getTenant();

        String context = getContext();

        Map<String, AttributeValue> key = getKey(tenant, context);

        GetItemRequest request = new GetItemRequest().withKey(key).withTableName(table);
        Map<String, AttributeValue> item = ddb.getItem(request).getItem();
        if (item == null) {
            throw new IntegrationStateException(STATE_NOT_FOUND);
        }
        AttributeValue state = item.get(DATA_FIELD);
        if (state == null) {
            throw new IntegrationStateException(STATE_PAYLOAD_NOT_FOUND);
        }
        String json = state.getS();

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, dataClass);
        } catch (JsonProcessingException e) {
            throw new IntegrationStateException(e);
        }
    }

    @Override
    public void update(String state, String stateMessage) {
        String tenant = getTenant();

        String context = getContext();

        Map<String, AttributeValue> key = getKey(tenant, context);

        UpdateItemRequest request = new UpdateItemRequest().withKey(key).withTableName(table);
        Map<String, AttributeValueUpdate> item = new HashMap<>();
        if (state != null) {
            item.put(STATE_FIELD, new AttributeValueUpdate(new AttributeValue(state), "PUT"));
            if (stateMessage != null) {
                item.put(STATE_MESSAGE_FIELD, new AttributeValueUpdate(new AttributeValue(stateMessage), "PUT"));
            }
        }
        request.withAttributeUpdates(item);
        LOGGER.info("Updating state {} with message {} for context {} for tenant {}.", state, stateMessage, key, tenant);
        ddb.updateItem(request);
    }

    @Override
    public void delete() {
        String tenant = getTenant();

        String context = getContext();

        Map<String, AttributeValue> key = getKey(tenant, context);

        DeleteItemRequest request = new DeleteItemRequest().withKey(key).withTableName(table);
        LOGGER.info("Deleting state for context {} for tenant {}.", key, tenant);
        ddb.deleteItem(request);
    }

    private String getTenant() {
        Object selector = exchange.getIn().getHeader(SELECTOR_HEADER);
        if (selector == null) {
            throw new IntegrationStateException(SELECTOR_HEADER_NOT_FOUND);
        }
        return selector.toString();
    }

    private String getContext() {
        Object currentContext = exchange.getMessage().getHeader(CONTEXT);
        if (currentContext == null) {
            throw new IntegrationStateException(CONTEXT_HEADER_NOT_FOUND);
        }
        return currentContext.toString();
    }

    private Map<String, AttributeValue> getKey(String tenant, String context) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(TENANT_FIELD, new AttributeValue(tenant));
        key.put(ID_FIELD, new AttributeValue(stateKey(context)));
        return key;
    }

    private String stateKey(String context) {
        return integrationName + ':' + context;
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
