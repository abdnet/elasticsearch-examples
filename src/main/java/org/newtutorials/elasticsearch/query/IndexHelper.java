package org.newtutorials.elasticsearch.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.newtutorials.elasticsearch.node.NodeBuilder;
import org.newtutorials.elasticsearch.pojo.ExampleDocument;

import java.util.concurrent.ExecutionException;

/**
 * Created by dani on 5/7/2017.
 */
public class IndexHelper {

    Node node;
    Client client;

    public IndexHelper() throws NodeValidationException {
        node = NodeBuilder.getNode(NodeBuilder.TEMP_FOLDER, NodeBuilder.CLUSTER_NAME, false);
        client = node.client();
    }

    public Client getClient() {
        return client;
    }

    public void createIndex(String index) throws NodeValidationException, InterruptedException, ExecutionException {
        ActionFuture<IndicesExistsResponse> indicesExistsResponseAction = client.admin().indices().exists(new IndicesExistsRequest(index));
        IndicesExistsResponse indicesExistsResponse = indicesExistsResponseAction.actionGet();
        if (!indicesExistsResponse.isExists())
        {
            ActionFuture<CreateIndexResponse> createIndexResponseAction = client.admin().indices().create(new CreateIndexRequest(index));
            CreateIndexResponse createIndexResponse = createIndexResponseAction.get();
            if (!createIndexResponse.isAcknowledged()) {
                throw new IllegalStateException("Failed to create index " + index);
            }
        }
    }

    public void putMapping(String index, String type, String mappingSource) throws NodeValidationException {
        PutMappingResponse mappingResponse = client.admin().indices().preparePutMapping(index)
                .setType(type)
                .setSource(mappingSource)
                .get();
        if (!mappingResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to create type " + index);
        }
    }

    public void deleteIndex(String index) throws NodeValidationException, InterruptedException, ExecutionException {
        client.admin().indices().prepareDelete(index).get();
    }

    public void createIndexAndAddDefaultItems(String indexName, String documentType) throws InterruptedException, ExecutionException, NodeValidationException, JsonProcessingException {
        createIndex(indexName);
        ObjectMapper objectMapper = new ObjectMapper();
        ExampleDocument exampleDocument;
        exampleDocument = new ExampleDocument("first document","Not Me",10);
        client.prepareIndex(indexName,documentType,"1").setSource(objectMapper.writeValueAsString(exampleDocument)).get();
        exampleDocument = new ExampleDocument("A nice book","Unknown Again",20);
        client.prepareIndex(indexName,documentType,"2").setSource(objectMapper.writeValueAsString(exampleDocument)).get();
        exampleDocument = new ExampleDocument("Old dusty","Author Again",50);
        client.prepareIndex(indexName,documentType,"3").setSource(objectMapper.writeValueAsString(exampleDocument)).get();
        exampleDocument = new ExampleDocument("New Car","Car Lover",55);
        client.prepareIndex(indexName,documentType,"4").setSource(objectMapper.writeValueAsString(exampleDocument)).get();

    }
}
