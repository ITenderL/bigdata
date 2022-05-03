package com.itender.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: ITender
 * @Date: 2022/03/20/ 11:39
 * @Description: flink批处理
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 文件路径
        String filePath = "E:\\workSpace\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\hello.text";
        DataSet<String> dataSet = env.readTextFile(filePath);
        // 对数据集进行处理，按空格分词展开，转换成（word, 1）二元数组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = dataSet.flatMap(new MyFlatMapper())
                // 按照第一个位置分组
                .groupBy(0)
                // 按照第二个位置求和
                .sum(1);

        resultSet.print();


    }
}
