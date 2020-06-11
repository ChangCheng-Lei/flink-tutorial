package com.flink.tutorial.window.triggers;

import lombok.Data;

/**
 * @author: leichangcheng
 * @date: 2020/6/11 09:44
 * @description:
 */
@Data
public class Word {
    private String word;
    private Long timeStamp;

    public Word(String word, long currentTimeMillis) {
        this.word = word;
        this.timeStamp = currentTimeMillis;
    }
}
