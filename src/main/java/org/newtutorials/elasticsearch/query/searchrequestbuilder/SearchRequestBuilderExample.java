package org.newtutorials.elasticsearch.query.searchrequestbuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.newtutorials.elasticsearch.pojo.ExampleDocument;
import org.newtutorials.elasticsearch.query.IndexHelper;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created by dani on 5/7/2017.
 */
public class SearchRequestBuilderExample {

    public static void main(String[] args) throws NodeValidationException, ExecutionException, InterruptedException, IOException {
        IndexHelper indexHelper = new IndexHelper();
        String indexName = "searchrequestbuilder";
        String documentType = "documents";
        indexHelper.createIndexAndAddDefaultItems(indexName,documentType);
        Client client = indexHelper.getClient();
        searchRequestBuilderMatchAllQuery(indexName,client);
    }

    private static void searchRequestBuilderMatchAllQuery(String indexName, Client client) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.readerFor(ExampleDocument.class);
        SearchRequestBuilder builder = client.prepareSearch(indexName);
        builder.setQuery(QueryBuilders.matchAllQuery());
        builder.addSort("pages", SortOrder.DESC);
        SearchResponse response = builder.get();
        for (SearchHit searchHit : response.getHits()) {
            ExampleDocument document= objectMapper.readValue(searchHit.getSourceAsString(), ExampleDocument.class);
            System.out.println(document);
        }
    }
}
