package com.lqs.common;

/**
 * @Author lqs
 * @Date 2022年04月11日 23:10:06
 * @Version 1.0.0
 * @ClassName GmallConfig
 * @Describe 常用的配置常量类GmallConfig
 */
public class GmallConfig {

    //TODO Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2204_REALTIME";

    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:nwh120,nwh121,nwh122:2181";

}
