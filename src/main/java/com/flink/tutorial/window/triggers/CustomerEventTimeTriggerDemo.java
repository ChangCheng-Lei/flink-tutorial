package com.flink.tutorial.window.triggers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: leichangcheng
 * @date: 2020/6/1 16:49
 * @description: 自定义处理时间触发器
 */
public class CustomerEventTimeTriggerDemo {
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
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new Trigger<Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> stringIntegerTuple2, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        System.out.println("onElement 元素<" + stringIntegerTuple2.f0 + "," + stringIntegerTuple2.f1 +
                                ">进入窗口 [" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")" + " long " + l);
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        System.out.println("onProcessingTime 窗口 [" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")" + "long" + l);
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        System.out.println("onEventTime 窗口 [" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")" + "long" + l);
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        System.out.println("clear 窗口 [" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")");
                    }
                })
                .sum(1)
                .print();
        environment.execute("Collect Word count");
        System.out.println("Event Time Trigger Trigger demo end");
    }
}
