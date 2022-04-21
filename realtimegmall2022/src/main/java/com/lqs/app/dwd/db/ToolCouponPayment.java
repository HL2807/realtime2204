package com.lqs.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.lqs.bean.CouponUsePay;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;
import java.util.Set;

/**
 * @Author lqs
 * @Date 2022年04月21日 21:22:09
 * @Version 1.0.0
 * @ClassName ToolCouponPayment
 * @Describe 工具域优惠券支付事物表
 * 需求：
 * 读取优惠券领用表数据，筛选优惠券支付数据，写入 Kafka 优惠券支付主题。
 * 思路步骤：
 * 用户使用优惠券支付时，优惠券领用表的 used_time 字段会更新为支付时间，因此优惠券支付数据应满足两个条件：
 * （1）操作类型为 update；
 * （2）修改了 used_time 字段。实现方式与前文同理。
 */
public class ToolCouponPayment {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、从Kafka读取业务数据，并封装为Flink SQL表
        tableEnvironment.executeSql("" +
                "create table `topic_db` ( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`old` string, " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_pay")
        );

        //TODO 3、读取优惠券领用表数据，封装为流
        Table couponUsePay = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['used_time'],'yyyy-MM-dd') date_id, " +
                "data['used_time'] used_time, " +
                "`old`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' "
        );
        DataStream<CouponUsePay> couponUsePayDS = tableEnvironment.toAppendStream(couponUsePay, CouponUsePay.class);

        //TODO 4、过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUsePay> filterDS = couponUsePayDS.filter(
                new FilterFunction<CouponUsePay>() {
                    @Override
                    public boolean filter(CouponUsePay value) throws Exception {
                        String old = value.getOld();
                        if (old != null) {
                            Map oldMap = JSON.parseObject(old, Map.class);
                            Set changeKeys = oldMap.keySet();
                            return changeKeys.contains("used_time");
                        }
                        return false;
                    }
                }
        );
        //简写版本
        /*SingleOutputStreamOperator<CouponUsePay> filteredDS = couponUsePayDS.filter(
                couponUsePayBean -> {
                    String old = couponUsePayBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("used_time");
                    }
                    return false;
                }
        );*/

        Table resultTable = tableEnvironment.fromDataStream(filterDS);
        tableEnvironment.createTemporaryView("result_table", resultTable);

        //TODO 6、创建upsert-kafka dwd_tool_coupon_pay 表
        tableEnvironment.executeSql("" +
                "create table dwd_tool_coupon_pay( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "payment_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_pay")
        );

        //TODO 6、将数据写入upsert-kafka表
        tableEnvironment.executeSql("" +
                "insert into dwd_tool_coupon_pay select " +
                "id, " +
                "coupon_id, " +
                "user_id, " +
                "order_id, " +
                "date_id, " +
                "used_time payment_time, " +
                "ts from result_table"
        ).print();

        env.execute("ToolCouponPayment");

    }

}
