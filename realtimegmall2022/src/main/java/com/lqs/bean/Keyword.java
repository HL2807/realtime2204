package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author lqs
 * @Date 2022年04月22日 20:19:48
 * @Version 1.0.0
 * @ClassName Keyword
 * @Describe 分词后的分词实体表
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Keyword {

    /** 窗口起始时间 */
    private String stt;
    /** 窗口闭合时间 */
    private String edt;
    /** 关键词来源 */
    private String source;
    /** 关键词 */
    private String keyword;
    /** 关键词出现频次 */
    private Long keyword_count;
    /** 时间戳 */
    private Long ts;

}
