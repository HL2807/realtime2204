package com.lqs.app.dwd.db;

import com.lqs.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月21日 16:00:15
 * @Version 1.0.0
 * @ClassName ToolCouponGet
 * @Describe 工具域获取优惠券领取事务事实表
 * 需求：读取优惠券领用数据，写入 Kafka 优惠券领用主题
 * <p>
 * 思路步骤：用户领取优惠券后，业务数据库的优惠券领用表会新增一条数据，因此操作类型为 insert 的数据即为优惠券领取数据。
 */
public class ToolCouponGet {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、从Kafka读取业务数据，封装为Flink Sql表
        tableEnvironment.executeSql("" +
                "create table `topic_db`( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_get")
        );

        //TODO 3、读取优惠券领用的数据，封装为表
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'], " +
                "data['coupon_id'], " +
                "data['user_id'], " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id, " +
                "data['get_time'], " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'insert' "
        );

        tableEnvironment.createTemporaryView("result_table", resultTable);

        //TODO 4、创建upsert-kafka dwd_tool_coupon_get表
        tableEnvironment.executeSql("" +
                "create table dwd_tool_coupon_get ( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "date_id string, " +
                "get_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_get")
        );

        //TODO 6、将数据写入upsert-kafka表
        tableEnvironment.executeSql(
                "insert into dwd_tool_coupon_get select * from result_table"
        ).print();

        env.execute("ToolCouponGet");

    }

}
