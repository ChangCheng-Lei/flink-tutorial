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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: leichangcheng
 * @date: 2020/6/11 16:49
 * @description: 自定义处理时间触发器
 */
public class CustomerProcessingTimeTriggerDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("Customer processing Time Trigger demo begin");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置使用处理时间
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<String> dataStream = environment.addSource(new RandomSource());
        dataStream.flatMap(new Tokenizer())
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
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
                        return TriggerResult.FIRE;
                    }

                    /**
                     * 针对时间处理，这个方法是不会触发的
                     * @param l
                     * @param timeWindow
                     * @param triggerContext
                     * @return
                     * @throws Exception
                     */
                    @Override
                    public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        System.out.println("onEventTime 窗口 [" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")" + "long" + l);
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                        System.out.println("clear 窗口 [" + timeWindow.getStart() + "," + timeWindow.getEnd() + ")");
                    }
                })
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
