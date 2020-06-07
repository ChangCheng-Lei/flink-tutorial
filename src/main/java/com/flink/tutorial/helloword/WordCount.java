package com.flink.tutorial.helloword;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: cc
 * @date: 2020/6/1 16:49
 * @description: 计算单词个数DEMO
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        System.out.println("Flink word count begin");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = environment.fromElements(WordCountData.WORDS);
        dataStream.flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1)
                .print();
        environment.execute("Collect Word count");
        System.out.println("Flink word count end");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
