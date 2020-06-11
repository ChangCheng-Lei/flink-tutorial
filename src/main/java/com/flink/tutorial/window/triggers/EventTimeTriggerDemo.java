package com.flink.tutorial.window.triggers;

import com.flink.tutorial.helloword.RandomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author: leichangcheng
 * @date: 2020/6/1 16:49
 * @description: 事件时间触发器
 */
public class EventTimeTriggerDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("Event Time Trigger demo begin");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Tuple2<String, Long>> dataStream = environment.addSource(new WordRandomSource());
        dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2) {
                return stringLongTuple2.f1;
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple2<String, Long> stringLongTuple2, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Tuple2<String, Integer> tuple2 = new Tuple2<>(stringLongTuple2.f0, 1);
                collector.collect(tuple2);
            }
        }).keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sum(1)
                .print();
        environment.execute("Collect Word count");
        System.out.println("Event Time Trigger Trigger demo end");
    }
}
