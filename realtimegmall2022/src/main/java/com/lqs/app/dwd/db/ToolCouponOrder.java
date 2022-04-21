package com.lqs.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.lqs.bean.CouponUseOrder;
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
 * @Date 2022年04月21日 16:08:52
 * @Version 1.0.0
 * @ClassName ToolCouponOrder
 * @Describe 工具域领取优惠券下单事务表
 * <p>
 * 需求：读取优惠券领用表数据，筛选优惠券下单数据，写入 Kafka 优惠券下单主题。
 * <p>
 * 思路步骤：用户使用优惠券下单时，优惠券领用表的 using_time 字段会更新为下单时间，因此优惠券下单数据应满足两个条件：
 * （1）操作类型为 update；
 * （2）修改了 using_time 字段。判断方式与前文同理。
 */
public class ToolCouponOrder {

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
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_order")
        );

        //TODO 3、从topic_db主题当中读取优惠券领用表数据，并封装为流
        Table couponUseOrder = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'] id, " +
                "data['coupon_id'] coupon_id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id, " +
                "data['using_time'] using_time, " +
                "`old`, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'coupon_use' " +
                "and `type` = 'update' "
        );

        DataStream<CouponUseOrder> couponUseOrderDS = tableEnvironment.toAppendStream(couponUseOrder, CouponUseOrder.class);

        //TODO 4、过滤满足条件的优惠券下单数据，封装为表
        SingleOutputStreamOperator<CouponUseOrder> filterDS = couponUseOrderDS.filter(
                new FilterFunction<CouponUseOrder>() {
                    @Override
                    public boolean filter(CouponUseOrder value) throws Exception {
                        String old = value.getOld();
                        if (old != null) {
                            Map oldMap = JSON.parseObject(old, Map.class);
                            Set changeKeys = oldMap.keySet();
                            return changeKeys.contains("using_time");
                        }
                        return false;
                    }
                }
        );
        //简写版本
        /*SingleOutputStreamOperator<CouponUseOrder> filteredDS = couponUseOrderDS.filter(
                couponUseOrderBean -> {
                    String old = couponUseOrderBean.getOld();
                    if (old != null) {
                        Map oldMap = JSON.parseObject(old, Map.class);
                        Set changeKeys = oldMap.keySet();
                        return changeKeys.contains("using_time");
                    }
                    return false;
                }
        );*/

        Table resultTable = tableEnvironment.fromDataStream(filterDS);
        tableEnvironment.createTemporaryView("result_table", resultTable);

        //TODO 5、创建upsert-kafka dwd_tool_coupon_order表
        tableEnvironment.executeSql("" +
                "create table dwd_tool_coupon_order( " +
                "id string, " +
                "coupon_id string, " +
                "user_id string, " +
                "order_id string, " +
                "date_id string, " +
                "order_time string, " +
                "ts string, " +
                "primary key(id) not enforced " +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_tool_coupon_order")
        );

        //TODO 6、将数据写入upsert-kafka表
        tableEnvironment.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id, " +
                "coupon_id, " +
                "user_id, " +
                "order_id, " +
                "date_id, " +
                "using_time order_time, " +
                "ts from result_table"
        ).print();

        env.execute("ToolCouponOrder");

    }

}
