package com.lqs.app.dwd.db;

import com.lqs.utils.KafkaUtil;
import com.lqs.utils.SqlUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月20日 21:40:12
 * @Version 1.0.0
 * @ClassName TradePaymentDetailSucess
 * @Describe 交易域支付成功事物表
 * 需求：从 Kafka topic_db筛选支付表数据并剔除与支付业务过程无关的数据、从dwd_trade_cart_add主题中读取订单事实表，
 * 关联两张表形成支付成功宽表，写入 Kafka 支付成功主题。
 * <p>
 * 思路步骤：
 * 1）筛选支付表数据并转化为流
 * 获取用户 id、支付类型、回调时间（支付成功时间）。
 * 生产环境下，用户支付后，业务数据库的支付表会插入一条数据，此时的回调时间和回调内容为空。通常底层会调用第三方支付接口，
 * 接口会返回回调信息，如果支付成功则回调信息不为空，此时会更新支付表，补全回调时间和回调内容字段的值，并将 payment_status
 * 字段的值修改为支付成功对应的状态码（本项目为 1602）。注：1602 在字典表中并没有对应记录，这是模拟数据的问题，并不影响业务逻辑，
 * 无须深究。
 * 由上述分析可知，支付成功对应的业务数据库变化日志应满足三个条件：（1）payment_status 字段的值为 1602；
 * （2）操作类型为 update；（3）更新的字段为 payment_status。
 * 前两个条件的判断在 Flink SQL 中完成，第三个条件的判断在流处理中完成。
 * 2）将支付成功流转化为动态表
 * 3）消费dwd_trade_cart_add主题数据创建动态表
 * 4）关联上述两张表形成支付成功宽表，写入 Kafka 支付成功主题
 */
public class TradePaymentDetailSuccess {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、读取业务数据，封装为Flink SQL 表
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
                ")" + KafkaUtil.getKafkaDDL("dwd_trade_order_detail", "dwd_trade_pay_detail")
        );

        //TODO 3、读取支付表数据
        tableEnvironment.executeSql(KafkaUtil.getTopicDbDDl("dwd_trade_pay_detail"));

        Table paymentInfoTable = tableEnvironment.sqlQuery("" +
                "select " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['payment_type'] payment_type, " +
                "data['callback_time'] callback_time, " +
                "`old`, " +
                "pt " +
                "from topic_db " +
                "where `table`='payment_info' " +
                "and `type`='update' " +
                "and data['payment_status']='1602' " +
                "and `old`['payment_status'] is not null"
        );

        tableEnvironment.createTemporaryView("payment_info",paymentInfoTable);

        //TODO 4、读取MySQL中的base_dic表构建维度表
        tableEnvironment.executeSql(SqlUtils.getBaseDicLookUpDDL());

        //TODO 5、关联三张表获得支付成功宽表
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select " +
                "    od.order_detail_id, " +
                "    od.order_id, " +
                "    od.user_id, " +
                "    od.sku_id, " +
                "    od.province_id, " +
                "    od.activity_id, " +
                "    od.activity_rule_id, " +
                "    od.coupon_id, " +
                "    pi.payment_type payment_type_code, " +
                "    dic.dic_name payment_type_name, " +
                "    pi.callback_time, " +
                "    od.source_id, " +
                "    od.source_type, " +
                "    od.sku_num, " +
                "    od.split_original_amount, " +
                "    od.split_activity_amount, " +
                "    od.split_coupon_amount, " +
                "    od.split_total_amount split_payment_amount, " +
                "    pi.pt " +
                "from payment_info pi " +
                "join dwd_trade_order_detail_table od " +
                "on pi.order_id = od.order_id " +
                "join base_dic FOR SYSTEM_TIME AS OF pi.pt dic " +
                "on pi.payment_type = dic.dic_code"
        );

        tableEnvironment.createTemporaryView("result_table",resultTable);

        //TODO 6、创建Kafka dwd_trade_pay_detail
        tableEnvironment.executeSql("" +
                        "create table dwd_trade_pay_detail_suc( " +
                        "order_detail_id string, " +
                        "order_id string, " +
                        "user_id string, " +
                        "sku_id string, " +
                        "province_id string, " +
                        "activity_id string, " +
                        "activity_rule_id string, " +
                        "coupon_id string, " +
                        "payment_type_code string, " +
                        "payment_type_name string, " +
                        "callback_time string, " +
                        "source_id string, " +
                        "source_type string, " +
                        "sku_num string, " +
                        "split_original_amount string, " +
                        "split_activity_amount string, " +
                        "split_coupon_amount string, " +
                        "split_payment_amount string, " +
                        "pt TIMESTAMP_LTZ(3) " +
                        ")" + KafkaUtil.getKafkaDDL("dwd_trade_pay_detail_suc","")
                );

        //TODO 7、将关联结果写入Upsert-Kafka表
        tableEnvironment.executeSql(
                "insert into dwd_trade_pay_detail_suc select * from result_table"
        ).print();

        env.execute("TradePaymentDetailSuccess");

    }

}
