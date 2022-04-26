package com.lqs.app.dws;

import com.lqs.app.functions.SplitFunction;
import com.lqs.bean.Keyword;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月22日 18:03:14
 * @Version 1.0.0
 * @ClassName TrafficSourceKeywordPageViewWindow
 * @Describe 流量域来源关键词粒度页面浏览各窗口汇总表
 * 需求：从 Kafka 页面浏览明细主题读取数据，过滤搜索行为，使用自定义 UDTF（一进多出）函数对搜索内容分词。
 * 统计各窗口各关键词出现频次，写入 ClickHouse。
 * 思路步骤：本程序将使用 FlinkSQL 实现。分词是个一进多出的过程，需要一个 UDTF 函数来实现，FlinkSQL 没有提供相关的内置函数，
 * 所以要自定义 UDTF 函数。
 * 自定义函数的逻辑在代码中实现，要完成分词功能，需要导入相关依赖，此处将借助 IK 分词器完成分词。
 * 最终要将数据写入 ClickHouse，需要补充相关依赖，封装 ClickHouse 工具类和方法。本节任务分为两部分：分词处理和数据写出。
 */
public class TrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、使用DDL方式读取DWD层页面浏览日志并创建表，同时获取事件时间生成Watermark
        tableEnvironment.executeSql("" +
                "create table page_log( " +
                "    `page` map<string,string>, " +
                "    `ts` bigint, " +
                "    `rt` as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ")" + KafkaUtil.getKafkaDDL("dwd_traffic_page_log", "Dws_Traffic_Source_Keyword_PageView_Window_211027")
        );

        //TODO 3、过滤出搜索数据
        Table keyWordTable = tableEnvironment.sqlQuery("" +
                "select " +
                "    page['item'] key_word, " +
                "    rt " +
                "from " +
                "    page_log " +
                "where page['item'] is not null " +
                "and page['last_page_id'] = 'search' " +
                "and page['item_type'] = 'keyword'"
        );

        tableEnvironment.createTemporaryView("key_word_table", keyWordTable);

        //TODO 4、使用自定义函数分词器处理
        //TODO 注册函数
        tableEnvironment.createTemporaryFunction("SplitFunction", SplitFunction.class);
        //TODO 处理数据
        Table splitTable = tableEnvironment.sqlQuery("" +
                "SELECT  " +
                "    word, " +
                "    rt " +
                "FROM key_word_table, LATERAL TABLE(SplitFunction(key_word))"
        );
        tableEnvironment.createTemporaryView("split_table",splitTable);

        //TODO 5、分组开窗聚合
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) keyword_count, " +
                "    UNIX_TIMESTAMP() ts " +
                "from " +
                "    split_table " +
                "group by TUMBLE(rt, INTERVAL '10' SECOND),word"
        );

        //TODO 6、将数据转换为流
        DataStream<Keyword> keywordDS = tableEnvironment.toAppendStream(resultTable, Keyword.class);

        keywordDS.print("KeywordDS>>>>>>");

        //TODO 7、将数据写出到ClickHouse
        keywordDS.addSink(ClickHouseUtil.getClickHouseSink(
                "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)"
        ));

        env.execute("TrafficSourceKeywordPageViewWindow");

    }

}
