package com.flink.tutorial.window.watermarks;

import com.flink.tutorial.window.triggers.WordDomain;
import com.flink.tutorial.window.triggers.WordRandomSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * @author: leichangcheng
 * @date: 2020/6/11 11:19
 * @description: 水位Demo
 */
public class WaterMarksDemo {
    /**
     * 参考链接  https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/event_timestamps_watermarks.html
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Water marks demo begin");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Tuple2<String, Long>> dataStream = environment.addSource(new WaterMarksSource());
        dataStream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<String, Long>>() {
            private final long maxOutOfOrderness = 3000; // 3 seconds, 最大延迟时间3秒
            private long currentMaxTimestamp;

            @Override
            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                Long timeStamp = stringLongTuple2.f1;
                currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
                return timeStamp;
            }

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(Tuple2<String, Long> stringLongTuple2, long l) {
                long timestamp = currentMaxTimestamp - maxOutOfOrderness;
                System.out.println("当前计算水位 :" + timestamp);
                return new Watermark(timestamp);
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple2<String, Long> stringLongTuple2, Collector<Tuple2<String, Integer>> collector) throws Exception {
                Tuple2<String, Integer> tuple2 = new Tuple2<>(stringLongTuple2.f0, 1);
                collector.collect(tuple2);
            }
        }).keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .allowedLateness(Time.seconds(1)) // 注释掉这一行就不会触发 延迟消息的计算， 在延迟后面的数据，每次到达一个就触发计算，不会受水位限制的影响
                .sum(1)
                .print();
        environment.execute("Collect Word count");
        System.out.println("Water marks demo end");
    }
}
