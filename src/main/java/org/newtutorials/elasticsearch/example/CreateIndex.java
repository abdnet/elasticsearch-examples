package org.newtutorials.elasticsearch.example;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.rest.RestStatus;
import org.newtutorials.elasticsearch.node.NodeBuilder;
import org.newtutorials.elasticsearch.util.FileUtil;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created by dani on 5/7/2017.
 */
public class CreateIndex {

    public static void main(String[] args) throws NodeValidationException, IOException, ExecutionException, InterruptedException {
        try {
            Node node = NodeBuilder.getNode(NodeBuilder.TEMP_FOLDER, NodeBuilder.CLUSTER_NAME, false);
            final String indexName = "newtutorials-examples";
            createIndex(indexName,node.client());
            final String documentsType = "documents";
            putMapping(indexName,node.client()
                    , documentsType
                    , "{\"properties\":{\"title\":{\"type\":\"string\"},\"author\":{\"type\":\"string\",\"index\":\"not_analyzed\"}}}");
            XContentBuilder document = createDocumentXContent();
            IndexResponse response = node.client().prepareIndex(indexName, documentsType)
                    .setId("1")
                    .setSource(document).get();
            if (response.status().getStatus() == RestStatus.CREATED.getStatus())
            {
                GetResponse getResponse = node.client().prepareGet(indexName,documentsType,"1").get();
                System.out.println(getResponse.toString());
            }
            deleteIndex(indexName,node.client());
            node.close();
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            FileUtil.deleteFolder(NodeBuilder.TEMP_FOLDER);
        }
    }

    private static XContentBuilder createDocumentXContent() throws IOException {
        return XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "Test document")
                        .field("author", "Elastic Search")
                        .field("pages", 100)
                        .endObject();
    }

    private static void createIndex(String index, Client client) throws NodeValidationException, InterruptedException, ExecutionException {
        ActionFuture<IndicesExistsResponse> indicesExistsResponseAction = client.admin().indices().exists(new IndicesExistsRequest("aaa"));
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

    private static void putMapping(String index, Client client, String type, String mappingSource) throws NodeValidationException {
        PutMappingResponse mappingResponse = client.admin().indices().preparePutMapping(index)
                .setType(type)
                .setSource(mappingSource)
                .get();
        if (!mappingResponse.isAcknowledged()) {
            throw new IllegalStateException("Failed to create type " + index);
        }
    }

    private static void deleteIndex(String index, Client client) throws NodeValidationException, InterruptedException, ExecutionException {
        client.admin().indices().prepareDelete(index).get();
    }
}