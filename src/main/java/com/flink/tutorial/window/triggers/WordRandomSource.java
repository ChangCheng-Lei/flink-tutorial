package com.flink.tutorial.window.triggers;

import com.flink.tutorial.helloword.RandomSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: leichangcheng
 * @date: 2020/5/13 16:14
 * @description:
 */
public class WordRandomSource extends RichSourceFunction<Tuple2<String, Long>> {

    private List<String> data = new ArrayList<>();

    public WordRandomSource() {
        data.add("qaz");
        data.add("wsx");
        data.add("edc");
        data.add("学习");
        data.add("上班");
    }


    @Override
    public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(100);
            String indexValue = data.get(i % data.size());
            Tuple2<String, Long> value = new Tuple2<>();
            value.setField(data.get(0), 0);
            value.setField(System.currentTimeMillis(), 1);
            sourceContext.collect(value);
        }
    }

    @Override
    public void cancel() {

    }
}
