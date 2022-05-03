package com.itender.flink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: ITender
 * @Date: 2022/05/03/ 0:41
 * @Description:
 */
public class FlinkElasticsearchSink {
    public static void main(String[] args) throws Exception {
        // 构建flink环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // source：数据来源
        DataStream<String> dataStream = env.socketTextStream("localhost", 8888);
        // 使用EsBuilder构建输出
        List<HttpHost> hostList = new ArrayList<>();
        hostList.add(new HttpHost("172.25.106.224", 9200, "http"));
        ElasticsearchSink.Builder<String> esBuilder = new ElasticsearchSink.Builder<String>(hostList, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                IndexRequest indexRequest = Requests.indexRequest();
                indexRequest.index("flink-index");
                Map<String, String> jsonMap = new HashMap<>(16);
                jsonMap.put("data", s);
                indexRequest.id("9001");
                indexRequest.source(jsonMap);
                System.out.println(jsonMap);
                requestIndexer.add(indexRequest);
            }
        });
        // sink: 数据输出
        esBuilder.setBulkFlushMaxActions(1);
        dataStream.addSink(esBuilder.build());
        // 执行操作
        env.execute("flink-es");
    }
}
