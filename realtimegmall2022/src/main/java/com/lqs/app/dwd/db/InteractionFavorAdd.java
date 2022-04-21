package com.lqs.app.dwd.db;

import com.lqs.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月21日 21:41:47
 * @Version 1.0.0
 * @ClassName InteractionFavorAdd
 * @Describe 互动域商品收藏事物表
 * <p>
 * 需求：
 * 读取收藏数据，写入 Kafka 收藏主题
 * 思路步骤：
 * 用户收藏商品时业务数据库的收藏表会插入一条数据，因此筛选操作类型为 update 的数据即可。
 */
public class InteractionFavorAdd {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、从Kafka读取业务数据，封装为Flink Sql表
        tableEnvironment.executeSql("" +
                "create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_favor_add")
        );

        //TODO 3、读取收藏表数据
        Table favorInfoTable = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "date_format(data['create_time'],'yyyy-MM-dd') date_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'favor_info' " +
                "and `type` = 'insert' " +
                "or (`type` = 'update' and `old`['is_cancel'] = '1' and data['is_cancel'] = '0')"
        );
        tableEnvironment.createTemporaryView("favor_info", favorInfoTable);

        //TODO 4、创建upsert-kafka dwd_interaction_favor_add表
        tableEnvironment.executeSql("" +
                "create table dwd_interaction_favor_add ( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "date_id string, " +
                "create_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_interaction_favor_add")
        );

        //TODO 5、将数据写入upsert-kafka表
        tableEnvironment.executeSql("" +
                "insert into dwd_interaction_favor_add select * from favor_info"
                ).print();

        env.execute("InteractionFavorAdd");

    }

}
