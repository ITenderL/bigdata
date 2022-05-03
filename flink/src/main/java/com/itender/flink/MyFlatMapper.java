package com.itender.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: ITender
 * @Date: 2022/03/20/ 11:47
 * @Description:
 */
public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] arr = s.split(" ");
        for (String s1 : arr) {
            collector.collect(new Tuple2<>(s1, 1));
        }
    }
}
