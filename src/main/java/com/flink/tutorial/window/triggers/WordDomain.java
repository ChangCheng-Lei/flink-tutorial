package com.flink.tutorial.window.triggers;

import lombok.Data;

/**
 * @author: leichangcheng
 * @date: 2020/6/11 09:44
 * @description:
 */
@Data
public class WordDomain {
    private String word;
    private Long timeStamp;

    public WordDomain(String word, long currentTimeMillis) {
        this.word = word;
        this.timeStamp = currentTimeMillis;
    }
}
