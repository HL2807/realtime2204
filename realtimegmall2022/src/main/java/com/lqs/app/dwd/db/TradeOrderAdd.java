package com.lqs.app.dwd.db;

import com.lqs.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月20日 11:59:38
 * @Version 1.0.0
 * @ClassName TradeOrderAdd
 * @Describe 交易域下单事物表
 * 需求：从 Kafka 读取订单事实表数据，过滤出创建订单的数据，写入 Kafka 对应主题
 * 实现步骤：
 * （1）从 Kafka  dwd_trade_cart_add主题读取业务数据；
 * （2）过滤流中符合条件的订单表数据：新增数据，即订单表的新增数据;
 * （3）写入 Kafka 下单主题。
 */
public class TradeOrderAdd {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、使用DDL的方式读取Kafka dwd_trade_order_detail 主题数据
        tableEnvironment.executeSql("" +
                "create table dwd_trade_order_detail_table( " +
                "    `order_detail_id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `order_create_time` string, " +
                "    `source_type` string, " +
                "    `source_id` string, " +
                "    `split_original_amount` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `pt` TIMESTAMP_LTZ(3), " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `activity_create_time` string , " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `coupon_create_time` string , " +
                "    `dic_name` string " +
                ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail","dwd_trade_order_add")
        );

        //TODO 3、过滤出下单数据
        Table filterTable = tableEnvironment.sqlQuery("" +
                "select " +
                "    * " +
                "from dwd_trade_order_detail_table " +
                "where `type`='insert'"
        );
        tableEnvironment.createTemporaryView("filter_table",filterTable);

        //TODO 4、创建Kafka下单数据表
        tableEnvironment.executeSql("" +
                        "create table dwd_trade_order_add_table( " +
                        "    `order_detail_id` string, " +
                        "    `order_id` string, " +
                        "    `sku_id` string, " +
                        "    `sku_name` string, " +
                        "    `order_price` string, " +
                        "    `sku_num` string, " +
                        "    `order_create_time` string, " +
                        "    `source_type` string, " +
                        "    `source_id` string, " +
                        "    `split_original_amount` string, " +
                        "    `split_total_amount` string, " +
                        "    `split_activity_amount` string, " +
                        "    `split_coupon_amount` string, " +
                        "    `pt` TIMESTAMP_LTZ(3), " +
                        "    `consignee` string, " +
                        "    `consignee_tel` string, " +
                        "    `total_amount` string, " +
                        "    `order_status` string, " +
                        "    `user_id` string, " +
                        "    `payment_way` string, " +
                        "    `out_trade_no` string, " +
                        "    `trade_body` string, " +
                        "    `operate_time` string, " +
                        "    `expire_time` string, " +
                        "    `process_status` string, " +
                        "    `tracking_no` string, " +
                        "    `parent_order_id` string, " +
                        "    `province_id` string, " +
                        "    `activity_reduce_amount` string, " +
                        "    `coupon_reduce_amount` string, " +
                        "    `original_total_amount` string, " +
                        "    `feight_fee` string, " +
                        "    `feight_fee_reduce` string, " +
                        "    `type` string, " +
                        "    `old` map<string,string>, " +
                        "    `activity_id` string, " +
                        "    `activity_rule_id` string, " +
                        "    `activity_create_time` string , " +
                        "    `coupon_id` string, " +
                        "    `coupon_use_id` string, " +
                        "    `coupon_create_time` string , " +
                        "    `dic_name` string " +
                        ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_add","")
                );

        //TODO 5、将数据写出到Kafka
        tableEnvironment.executeSql(
                "insert into dwd_trade_order_add_table select * from filter_table"
        ).print();

        env.execute("TradeOrderAdd");

    }

}
