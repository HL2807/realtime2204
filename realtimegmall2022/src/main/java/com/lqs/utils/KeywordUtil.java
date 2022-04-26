package com.lqs.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年04月22日 18:16:38
 * @Version 1.0.0
 * @ClassName KeywordUtil
 * @Describe 搜索分词，ik分词器工具类
 */
public class KeywordUtil {

    public static List<String> splitKeyword(String keyword) throws IOException {

        //TODO 创建集合用于存放结果数据
        ArrayList<String> result = new ArrayList<>();

        //TODO 创建分词器对象
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //TODO 提取分词
        Lexeme next = ikSegmenter.next();

        while (next!=null){
            String word = next.getLexemeText();
            result.add(word);

            next = ikSegmenter.next();
        }

        return result;

    }

}
