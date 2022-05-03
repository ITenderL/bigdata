package com.itender.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: ITender
 * @Date: 2022/03/20/ 11:58
 * @Description: flink流式处理
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 文件路径
        // String filePath = "E:\\workSpace\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\hello.text";
        // DataStream<String> dataStream = env.readTextFile(filePath);

        // 从socket文本流读取数据
        DataStream<String> dataStream = env.socketTextStream("localhost", 7777);

        // 基于数据流进行数据处理
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.flatMap(new MyFlatMapper()).keyBy(0).sum(1);
        resultStream.print();
        env.execute();
    }
}
