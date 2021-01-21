package com.xizi.poem;

import com.github.houbb.segment.support.segment.result.impl.SegmentResultHandlers;
import com.github.houbb.segment.util.SegmentHelper;

import java.io.IOException;
import java.util.List;

public class TestAnalyzer {
    public static void main(String[] args) throws IOException {
        String text="曾经沧海难为水";
        //创建分词对象
        List<String> segment = SegmentHelper.segment(text, SegmentResultHandlers.word());
        for (String s : segment) {
            System.out.println("s = " + s);
        }

    }
}
