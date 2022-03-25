package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-23 17:55
 */
public class KeywordUtil {
    public static List<String> analyze(String keyword) throws IOException {

        //创建用于存放结果的
        ArrayList<String> result = new ArrayList<>();

        //获取IK分词对象
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword), false);

        Lexeme next = ikSegmenter.next();
        while (next != null){
            String word = next.getLexemeText();
            result.add(word);
            next = ikSegmenter.next();
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        List<String> strings = analyze("上海大数据210927实时数仓");
        System.out.println(strings);
    }
}
