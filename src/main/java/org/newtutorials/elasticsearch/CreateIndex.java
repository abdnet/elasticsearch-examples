package org.newtutorials.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
/**
 * Created by dani on 5/4/2017.
 */
public class CreateIndex {
    public static void main(String[] args) {
        Node node = NodeBuilder.nodeBuilder()
                .clusterName("newtutorials-elasticsearch")
                .settings(Settings.builder()
                                .put("path.home", ".")
                                .put("index.store.type", "memory")
                )
//                .client(true)
                .node();
        Client client = node.client();
//        client.admin().indices().
    }
}
