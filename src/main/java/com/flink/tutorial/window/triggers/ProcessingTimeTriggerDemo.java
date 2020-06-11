package com.flink.tutorial.window.triggers;

import com.flink.tutorial.helloword.RandomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.util.Collector;

/**
 * @author: leichangcheng
 * @date: 2020/6/1 16:49
 * @description: ProcessingTimeTrigger 处理时间触发器
 */
public class ProcessingTimeTriggerDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("Processing Time Trigger demo begin");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置使用处理时间
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<String> dataStream = environment.addSource(new RandomSource());
        dataStream.flatMap(new Tokenizer())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(100)))
                .sum(1)
                .print();
        environment.execute("Collect Word count");
        System.out.println("Processing Time Trigger demo end");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                out.collect(new Tuple2<>(value, 1));
        }
    }
}
