import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 功 能:  老es索引数据迁移到新的es上，新es上的索引必须存在，并且和老的完全一致
 * 创建人: LDL
 * 时 间:  2015/12/29 11:08
 */
public class DataMigration {

    static final Settings settings = ImmutableSettings.settingsBuilder()
            .put("client.transport.sniff", true).put("cluster.name", "es").build();

    final static Client srcClient = new TransportClient()
            .addTransportAddress(new InetSocketTransportAddress("192.168.1.211", 9300));

    final static Client targetClient = new TransportClient(settings)
            .addTransportAddress(new InetSocketTransportAddress("192.168.1.212", 9300))
            .addTransportAddress(new InetSocketTransportAddress("192.168.1.213", 9300));

    private static final String index = "cms_data_saibo";
    private static final String type = "info";

    public static void main(String[] args) throws IOException {

        final List<Map<String, Object>> sourceMaps = new ArrayList<>();
        final SearchRequestBuilder searchRequestBuilder = srcClient.prepareSearch(index)
                .setTypes(type)
                .setFrom(0)
                .setSize(756);
        final SearchResponse response = searchRequestBuilder
                .execute()
                .actionGet();

        final SearchHits searchHits = response.getHits();
        for (int i = 0; i < searchHits.getHits().length; i++) {
            final SearchHit searchHit = searchHits.getHits()[i];
            final Map<String, Object> map = searchHit.sourceAsMap();
            map.put("_id", searchHit.getId());
            sourceMaps.add(map);
        }

        final BulkRequestBuilder bulkRequest = targetClient.prepareBulk();

        for (Map<String, Object> sourceMap : sourceMaps) {
            final XContentBuilder xContentBuilder = jsonBuilder()
                    .startObject();
            for (String key : sourceMap.keySet()) {
                xContentBuilder.field(key, sourceMap.get(key));
            }
            xContentBuilder.endObject();
            bulkRequest.add(targetClient.prepareIndex(index, type, sourceMap.get("_id").toString())
                    .setSource(xContentBuilder)
            );
        }

        final BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        System.out.println(bulkResponse.hasFailures());
    }
}
