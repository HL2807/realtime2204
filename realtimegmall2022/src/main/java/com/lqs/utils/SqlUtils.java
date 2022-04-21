package com.lqs.utils;

/**
 * @Author lqs
 * @Date 2022年04月18日 15:53:56
 * @Version 1.0.0
 * @ClassName SqlUtils
 * @Describe mysql 读取工具类
 */
public class SqlUtils {

    public static String getBaseDicLookUpDDL() {
        return "create table `base_dic`( " +
                "`dic_code` string, " +
                "`dic_name` string, " +
                "`parent_code` string, " +
                "`create_time` timestamp, " +
                "`operate_time` timestamp, " +
                "primary key(`dic_code`) not enforced " +
                ")" + SqlUtils.mysqlLookUpTableDDL("base_dic");
    }

    public static String mysqlLookUpTableDDL(String tableName) {
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://nwh120:3306/gmall2204', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '100', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = '912811', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

}
