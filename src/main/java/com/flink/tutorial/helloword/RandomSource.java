package com.flink.tutorial.helloword;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: leichangcheng
 * @date: 2020/5/13 16:14
 * @description:
 */
public class RandomSource extends RichSourceFunction<String> {

    private List<String> data = new ArrayList<>();

    public RandomSource() {
        data.add("qaz");
        data.add("wsx");
        data.add("edc");
        data.add("学习");
        data.add("上班");
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        for (int i = 0 ; i < 1000 ; i ++){
            Thread.sleep(100);
            sourceContext.collect(data.get(i%data.size()));
        }
    }

    @Override
    public void cancel() {

    }
}
