package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author NaZFeng
 * @create 2020-07-11 11:12
 */
public class EsReader {
    public static void main(String[] args) throws IOException {
//        JestClientFactory jestClientFactory = new JestClientFactory();
//
//        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop102:9200").build();
//
//        jestClientFactory.setHttpClientConfig(config);
//
//        JestClient jestClient = jestClientFactory.getObject();

        JestClientFactory jestClientFactory = new JestClientFactory();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        jestClientFactory.setHttpClientConfig(httpClientConfig);

        JestClient jestClient = jestClientFactory.getObject();

        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"favo\": \"球\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}")
                .addIndex("people")
                .addType("_doc")
                .build();

        SearchResult searchResult = jestClient.execute(search);

        System.out.println("总数："+ searchResult.getTotal());
        System.out.println("max_score："+ searchResult.getMaxScore());

        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            for (Object key : source.keySet()) {
                System.out.println("Key:"+key+",Value:"+source.get(key));
            }
        }

        System.out.println("*********************************");

        jestClient.close();
    }


}
