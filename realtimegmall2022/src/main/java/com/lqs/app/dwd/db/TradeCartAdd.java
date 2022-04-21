package com.lqs.app.dwd.db;

import com.lqs.utils.KafkaUtil;
import com.lqs.utils.SqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月18日 15:35:31
 * @Version 1.0.0
 * @ClassName TradeCartAdd
 * @Describe 交易域加购事务事实表
 * 需求：提取加购操作生成加购表，并将字典表中的相关维度退化到加购表中，写出到 Kafka 对应主题。
 */
public class TradeCartAdd {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 设置状态时间
        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        //TODO 2、使用DDL方式读取Kafka topic_db主题数据
        tableEnvironment.executeSql(KafkaUtil.getTopicDbDDl("dwd_trade_cart_add"));

        //打印测试
        //Table table = tableEnv.sqlQuery("select * from topic_db");
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        //rowDataStream.print(">>>>>>>>>>>");

        //TODO 3、过滤出加购数据
        Table cartAddTable = tableEnvironment.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['user_id'] user_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['cart_price'] cart_price, " +
                "    if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num, " +
                "    data['sku_name'] sku_name, " +
                "    data['is_checked'] is_checked, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['is_ordered'] is_ordered, " +
                "    data['order_time'] order_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database`='gmall2204' " +
                "and `table`='cart_info' " +
                "and (`type`='insert'  " +
                "    or (`type`='update'  " +
                "        and `old`['sku_num'] is not null " +
                "        and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))"
        );
        tableEnvironment.createTemporaryView("cart_add",cartAddTable);

        //打印测试
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(cartAddTable, Row.class);
        //rowDataStream.print(">>>>>>>>>>>");

        //TODO 4、读取MySQL中的base_dic
        tableEnvironment.executeSql(SqlUtils.getBaseDicLookUpDDL());

        //打印测试
        //Table table = tableEnv.sqlQuery("select * from base_dic");
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        //rowDataStream.print(">>>>>>>>>>>");

        //TODO 5、关联两张表，维度退化
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select " +
                "    c.id, " +
                "    c.user_id, " +
                "    c.sku_id, " +
                "    c.cart_price, " +
                "    c.sku_num, " +
                "    c.sku_name, " +
                "    c.is_checked, " +
                "    c.create_time, " +
                "    c.operate_time, " +
                "    c.is_ordered, " +
                "    c.order_time, " +
                "    c.source_type, " +
                "    c.source_id, " +
                "    b.dic_name " +
                "from cart_add c " +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt as b " +
                "on c.source_type = b.dic_code"
        );

        tableEnvironment.createTemporaryView("result_table",resultTable);

        //打印测试
        //Table table = tableEnv.sqlQuery("select * from result_table");
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        //rowDataStream.print(">>>>>>>>>>>");

        //TODO 6、将数据写回到Kafka DWD层
        String sinkTopic = "dwd_trade_cart_add";

        tableEnvironment.executeSql("" +
                "create table trade_cart_add( " +
                "    id string, " +
                "    user_id string, " +
                "    sku_id string, " +
                "    cart_price string, " +
                "    sku_num int, " +
                "    sku_name string, " +
                "    is_checked string, " +
                "    create_time string, " +
                "    operate_time string, " +
                "    is_ordered string, " +
                "    order_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    dic_name string " +
                ")" + KafkaUtil.getKafkaDDL(sinkTopic, "")
        );

        tableEnvironment.executeSql("insert into trade_cart_add select * from result_table")
                .print();

        env.execute("DwdTradeCartAdd");

    }

}
