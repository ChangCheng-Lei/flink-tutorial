package com.flink.tutorial.window.watermarks;

import com.flink.tutorial.window.triggers.WordDomain;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: leichangcheng
 * @date: 2020/5/13 16:14
 * @description: 水印数据源
 */
public class WaterMarksSource extends RichSourceFunction<Tuple2<String, Long>> {

    private List<String> data = new ArrayList<>();

    @Override
    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
        LocalDateTime startDate = LocalDateTime.now();

        /**
         * 模拟第一个窗口数据
         */
        String value = "第一个窗口数据";
        Long time = Long.valueOf(startDate.toInstant(ZoneOffset.of("+8")).toEpochMilli());
        sourceContext.collect(new Tuple2<String, Long>(value, time));
        System.out.println("收集： " + value + "时间: " + time);
        sourceContext.collect(new Tuple2<String, Long>(value, time));
        System.out.println("收集： " + value + "时间: " + time);

        /**
         * 模拟第二个窗口数据
         */
        LocalDateTime addDate = startDate.plusSeconds(1);
        value = "第二个窗口数据";
        time = addDate.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        sourceContext.collect(new Tuple2<String, Long>(value, time));
        System.out.println("收集： " + value + "时间: " + time);

        /**
         * 模拟第四个窗口数据先到
         */
        addDate = startDate.plusSeconds(4);
        value = "第四个窗口数据";
        time = addDate.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        sourceContext.collect(new Tuple2<String, Long>(value, time));
        System.out.println("收集数据： " + value + "时间: " + time);

        /**
         * 模拟第三个窗口数据后到
         */
        addDate = startDate.plusSeconds(3);
        value = "第三个窗口数据";
        time = addDate.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        sourceContext.collect(new Tuple2<String, Long>(value, time));
        System.out.println("收集数据： " + value + "时间: " + time);

        Thread.sleep(2000);
        System.out.println("等待2秒");

        /**
         * 模拟第七个窗口数据数据到达，此时触发第三个窗口计算
         */
        addDate = startDate.plusSeconds(7);
        value = "第七个窗口数据";
        time = addDate.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        sourceContext.collect(new Tuple2<String, Long>(value, time));
        System.out.println("收集数据： " + value + "时间: " + time);

        Thread.sleep(2000);
        System.out.println("等待2秒");

        /**
         * 模拟第三个窗口数据后到
         */
        addDate = startDate.plusSeconds(3);
        value = "第三个窗口数据";
        time = addDate.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        sourceContext.collect(new Tuple2<String, Long>(value, time));
        System.out.println("收集数据： " + value + "时间: " + time);

        Thread.sleep(20000);
    }

    @Override
    public void cancel() {

    }
}
