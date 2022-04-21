package com.lqs.app.dwd.db;

import com.lqs.utils.KafkaUtil;
import com.lqs.utils.SqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月21日 21:49:00
 * @Version 1.0.0
 * @ClassName InteractionComment
 * @Describe 互动域评价事物表
 * <p>
 * 需求：
 * 建立 MySQL-Lookup 字典表，读取评论表数据，关联字典表以获取评价（好评、中评、差评、自动），将结果写入 Kafka 评价主题。
 * 思路步骤：
 * 用户提交评论时评价表会插入一条数据，筛选操作类型为 insert 的数据即可。
 */
public class InteractionComment {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、从Kafka中读取业务数据，并封装为Flink Sql表
        tableEnvironment.executeSql("" +
                "create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_comment")
        );

        //TODO 3、读取评价表数据
        Table commentInfoTable = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "data['order_id'] order_id, " +
                "data['create_time'] create_time, " +
                "data['appraise'] appraise, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'comment_info' " +
                "and `type` = 'insert' "
        );
        tableEnvironment.createTemporaryView("comment_info", commentInfoTable);

        //TODO 4、创建mysql-lookup字典表
        tableEnvironment.executeSql(SqlUtils.getBaseDicLookUpDDL());

        //TODO 5、关联两张表
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select " +
                "ci.id, " +
                "ci.user_id, " +
                "ci.sku_id, " +
                "ci.order_id, " +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id, " +
                "ci.create_time, " +
                "ci.appraise, " +
                "dic.dic_name, " +
                "ts " +
                "from comment_info ci " +
                "left join " +
                "base_dic for system_time as of ci.proc_time as dic " +
                "on ci.appraise = dic.dic_code"
        );
        tableEnvironment.createTemporaryView("result_table",resultTable);

        //TODO 6、创建upsert-kafka dwd_interaction_comment表
        tableEnvironment.executeSql("" +
                "create table dwd_interaction_comment( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "order_id string, " +
                "date_id string, " +
                "create_time string, " +
                "appraise_code string, " +
                "appraise_name string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_interaction_comment")
        );

        //TODO 7、将关联后的数据写入upsert-kafka表
        tableEnvironment.executeSql(
                "insert into dwd_interaction_comment select * from result_table"
        ).print();

        env.execute("InteractionComment");

    }

}
