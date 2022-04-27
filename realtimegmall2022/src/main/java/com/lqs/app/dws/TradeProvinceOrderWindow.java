package com.lqs.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.lqs.app.functions.DimAsyncFunction;
import com.lqs.app.functions.OrderDetailFilterFunction;
import com.lqs.bean.TradeProvinceOrderWindowBean;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author lqs
 * @Date 2022年04月27日 11:38:07
 * @Version 1.0.0
 * @ClassName TradeProvinceOrderWindow
 * @Describe 交易域省份粒度下单各窗口汇总表
 *
 * 需求：从 Kafka 读取订单明细数据，过滤 null 数据并按照唯一键对数据去重，统计各省份各窗口订单数和订单金额，
 * 将数据写入 ClickHouse 交易域省份粒度下单各窗口汇总表。
 *
 * 思路步骤：
 * 1）从 Kafka 订单明细主题读取数据
 * 2）过滤 null 数据并转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * 	JSONObject 转换为实体类 TradeProvinceOrderWindow。
 * 5）设置水位线
 * 6）按照省份 ID 和省份名称分组
 * 此时的 provinceName 均为空字符串，但 provinceId 已经可以唯一标识省份，因此不会影响计算结果。
 * 本程序将省份名称字段的补全放在聚合之后，聚合后的数据量显著减少，这样做可以大大节省资源开销，提升性能。
 * 7）开窗
 * 8）聚合计算
 * 度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * 9）关联省份信息
 * 补全省份名称字段。
 * 10）写出到 ClickHouse。
 */
public class TradeProvinceOrderWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取kafka DWD层order_detail数据，并去重过滤
        String groupId = "dws_trade_province_order_window";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjectDS = OrderDetailFilterFunction.getDwOrderDetail(env, groupId);

        //TODO 3、将每行数据转换为JavaBean
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> tradeProvinceDS = orderDetailJsonObjectDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderWindowBean>() {
                    @Override
                    public TradeProvinceOrderWindowBean map(JSONObject value) throws Exception {
                        return new TradeProvinceOrderWindowBean(
                                "",
                                "",
                                value.getString("province_id"),
                                "",
                                1L,
                                value.getDouble("split_total_amount"),
                                DateFormatUtil.toTs(value.getString("order_create_time"), true)
                        );
                    }
                }
        );

        //TODO 4、提取时间戳生成WaterMark
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> tradeProvinceWithWaterMarkDS = tradeProvinceDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderWindowBean>() {
                                    @Override
                                    public long extractTimestamp(TradeProvinceOrderWindowBean element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 5、分组开窗聚合
        KeyedStream<TradeProvinceOrderWindowBean, String> keyedStream = tradeProvinceWithWaterMarkDS.keyBy(
                new KeySelector<TradeProvinceOrderWindowBean, String>() {
                    @Override
                    public String getKey(TradeProvinceOrderWindowBean value) throws Exception {
                        return value.getProvinceId();
                    }
                }
        );

        //TODO 开窗
        WindowedStream<TradeProvinceOrderWindowBean, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //TODO 聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> resultDS = windowedStream.reduce(
                new ReduceFunction<TradeProvinceOrderWindowBean>() {
                    @Override
                    public TradeProvinceOrderWindowBean reduce(TradeProvinceOrderWindowBean value1, TradeProvinceOrderWindowBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderWindowBean, TradeProvinceOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindowBean> input, Collector<TradeProvinceOrderWindowBean> out) throws Exception {

                        TradeProvinceOrderWindowBean tradeProvinceOrderWindowBean = input.iterator().next();

                        tradeProvinceOrderWindowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeProvinceOrderWindowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradeProvinceOrderWindowBean.setTs(System.currentTimeMillis());

                        out.collect(tradeProvinceOrderWindowBean);

                    }
                }
        );

        //TODO 6、关联维表获取省份名称
        SingleOutputStreamOperator<TradeProvinceOrderWindowBean> resultJoinDBPDS = AsyncDataStream.unorderedWait(
                resultDS,
                new DimAsyncFunction<TradeProvinceOrderWindowBean>("DIM_BASE_PROVINCE") {

                    @Override
                    public String getKey(TradeProvinceOrderWindowBean input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindowBean input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setProvinceName(dimInfo.getString("NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 7、将数据写入ClickHouse
        resultJoinDBPDS.print("resultJoinDBPDS>>>>>>");
        resultJoinDBPDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"
                )
        );

        env.execute("TradeProvinceOrderWindow");

    }

}
