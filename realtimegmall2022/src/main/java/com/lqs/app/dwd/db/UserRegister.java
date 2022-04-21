package com.lqs.app.dwd.db;

import com.lqs.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月21日 22:01:08
 * @Version 1.0.0
 * @ClassName UserRegister
 * @Describe 用户域用户注册事务表
 * 需求：
 * 读取用户表数据，获取注册时间，将用户注册信息写入 Kafka 注册主题。
 * 思路步骤：
 * 用户注册时会在用户表中插入一条数据，筛选操作类型为 insert 的数据即可。
 */
public class UserRegister {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、读取Kafka业务数据，并封装为Flink Sql表
        tableEnvironment.executeSql("" +
                "create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_detail")
        );

        //TODO 3、读取用户数据表
        Table userInfoTable = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'] user_id, " +
                "data['create_time'] create_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'user_info' " +
                "and `type` = 'insert' "
        );
        tableEnvironment.createTemporaryView("user_info", userInfoTable);

        //TODO 4、创建upsert-kafka dwd_user_register表
        tableEnvironment.executeSql("" +
                "create table `dwd_user_register`( " +
                "`user_id` string, " +
                "`date_id` string, " +
                "`create_time` string, " +
                "`ts` string, " +
                "primary key(`user_id`) not enforced " +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_user_register")
        );

        //TODO 5、将输入写入upsert-kafka表
        tableEnvironment.executeSql("" +
                "insert into dwd_user_register " +
                "select  " +
                "user_id, " +
                "date_format(create_time, 'yyyy-MM-dd') date_id, " +
                "create_time, " +
                "ts " +
                "from user_info"
        ).print();

        env.execute("UserRegister");

    }

}
