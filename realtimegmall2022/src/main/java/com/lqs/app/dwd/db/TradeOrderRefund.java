package com.lqs.app.dwd.db;

import com.alibaba.fastjson.JSON;
import com.lqs.bean.OrderInfoRefund;
import com.lqs.utils.KafkaUtil;
import com.lqs.utils.SqlUtils;
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
 * @Date 2022年04月20日 21:53:51
 * @Version 1.0.0
 * @ClassName TradeOrderRefund
 * @Describe 交易域退单事物表
 * 需求：
 * 从 Kafka 读取业务数据，筛选退单表数据，筛选满足条件的订单表数据，建立 MySQL-Lookup 字典表，关联三张表获得退单明细宽表。
 * 思路步骤：
 * 1）筛选退单表数据
 * 退单业务过程最细粒度的操作为一个订单中一个 SKU 的退单操作，退单表粒度与最细粒度相同，将其作为主表。
 * 2）筛选订单表数据并转化为流
 * 获取 province_id。退单操作发生时，订单表的 order_status 字段值会被更新为 1005。订单表中的数据要满足三个条件：
 * （1）order_status 为 1005（退款中）；
 * （2）操作类型为 update；
 * （3）更新的字段为 order_status。前两个条件的判断在 Flink SQL 中完成，第三个条件的判断在流处理中完成。
 * 3）将筛选后的订单表退单数据流转化为 Flink SQL 动态表
 * 4）建立 MySQL-Lookup 字典表
 * 获取退款类型名称和退款原因类型名称
 * 5）关联这几张表获得退单明细宽表，写入 Kafka 退单明细主题
 * 主表中的数据都与退单业务相关，因此所有关联均用左外联即可。第二步是否对订单表数据筛选并不影响查询结果，
 * 提前对数据进行过滤是为了减少数据量，减少性能消耗。下文同理，不再赘述。
 */
public class TradeOrderRefund {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、从Kafka读取topic_db 数据封装为Flink Sql表
        tableEnvironment.executeSql("" +
                "create table topic_db( " +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` string, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_refund")
        );

        //TODO 3、读取退单表数据
        Table orderRefundInfoTable = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['refund_type'] refund_type, " +
                "data['refund_num'] refund_num, " +
                "data['refund_amount'] refund_amount, " +
                "data['refund_reason_type'] refund_reason_type, " +
                "data['refund_reason_txt'] refund_reason_txt, " +
                "data['create_time'] create_time, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'order_refund_info' " +
                "and `type` = 'insert' "
        );
        tableEnvironment.createTemporaryView("order_refund_info", orderRefundInfoTable);

        //TODO 4、读取订单表数据并转换为流
        Table orderInfo = tableEnvironment.sqlQuery("" +
                "select " +
                "data['id'] id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and data['order_status']='1005'"
        );

        DataStream<OrderInfoRefund> orderInfoRefundDS = tableEnvironment.toAppendStream(orderInfo, OrderInfoRefund.class);

        //TODO 5、过滤符合条件的订单表的退单数据
        SingleOutputStreamOperator<OrderInfoRefund> filterDS = orderInfoRefundDS.filter(
                new FilterFunction<OrderInfoRefund>() {
                    @Override
                    public boolean filter(OrderInfoRefund value) throws Exception {
                        String old = value.getOld();
                        if (old != null) {
                            Map odlMap = JSON.parseObject(old, Map.class);
                            Set changeKeys = odlMap.keySet();
                            return changeKeys.contains("order_status");
                        }
                        return false;
                    }
                }
        );
        //简写
        /*SingleOutputStreamOperator<OrderInfoRefund> filterDS = orderInfoRefundDS.filter(value -> {
            String old = value.getOld();
            if (old != null) {
                Map odlMap = JSON.parseObject(old, Map.class);
                Set changeKeys = odlMap.keySet();
                return changeKeys.contains("order_status")
            }
            return false;
        });*/

        //TODO 6、将订单退单流转换为表
        Table orderInfoRefund = tableEnvironment.fromDataStream(filterDS);
        tableEnvironment.createTemporaryView("order_info_refund", orderInfoRefund);

        //TODO 7、创建MySQL lookup 字典表
        tableEnvironment.executeSql(SqlUtils.getBaseDicLookUpDDL());

        //TODO 8、关联三张表获得退单宽表
        Table resultTable = tableEnvironment.sqlQuery("" +
                "select  " +
                "ri.id, " +
                "ri.user_id, " +
                "ri.order_id, " +
                "ri.sku_id, " +
                "oi.province_id, " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
                "ri.create_time, " +
                "ri.refund_type, " +
                "type_dic.dic_name, " +
                "ri.refund_reason_type, " +
                "reason_dic.dic_name, " +
                "ri.refund_reason_txt, " +
                "ri.refund_num, " +
                "ri.refund_amount, " +
                "ri.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_refund_info ri " +
                "left join  " +
                "order_info_refund oi " +
                "on ri.order_id = oi.id " +
                "left join  " +
                "base_dic for system_time as of ri.proc_time as type_dic " +
                "on ri.refund_type = type_dic.dic_code " +
                "left join " +
                "base_dic for system_time as of ri.proc_time as reason_dic " +
                "on ri.refund_reason_type=reason_dic.dic_code"
        );

        tableEnvironment.createTemporaryView("result_table", resultTable);

        //TODO 9、创建upsert-kafka dwd_trade_order_refund表
        tableEnvironment.executeSql("" +
                "create table dwd_trade_order_refund( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "date_id string, " +
                "create_time string, " +
                "refund_type_code string, " +
                "refund_type_name string, " +
                "refund_reason_type_code string, " +
                "refund_reason_type_name string, " +
                "refund_reason_txt string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " +
                ")" + KafkaUtil.getUpsertKafkaDDL("dwd_trade_order_refund")
        );

        //TODO 10、将关联结果数据写入upsert-kafka表
        tableEnvironment.executeSql(
                "insert into dwd_trade_order_refund select * from result_table"
        ).print();

        env.execute("TradeOrderRefund");

    }

}
