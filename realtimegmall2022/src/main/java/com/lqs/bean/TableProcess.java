package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author lqs
 * @Date 2022年04月11日 21:55:55
 * @Version 1.0.0
 * @ClassName TableProcess
 * @Describe HBase mysql配置表实体类
 */
@Data
public class TableProcess {

    /** 来源表*/
    String sourceTable;
    /** 输出表*/
    String sinkTable;
    /** 输出字段*/
    String sinkColumns;
    /** 主键字段*/
    String sinkPk;
    /** 建表扩展*/
    String sinkExtend;

}
