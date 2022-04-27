package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.TradePaymentWindow;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月27日 15:48:27
 * @Version 1.0.0
 * @ClassName TradePaymentSucessWindow
 * @Describe 交易域支付成功各窗口汇总表
 *
 * 需求：
 * 从 Kafka 读取交易域支付成功主题数据，统计支付成功独立用户数和首次支付成功用户数。
 *
 * 思路步骤：
 * 1）从 Kafka 支付成功明细主题读取数据
 * 2）过滤为 null 的数据，转换数据结构
 * 	String 转换为 JSONObject。
 * 3）按照唯一键分组
 * 4）去重
 * 	与前文同理。
 * 5）设置水位线，按照 user_id 分组
 * 6）统计独立支付人数和新增支付人数
 * 	运用 Flink 状态编程，在状态中维护用户末次支付日期。
 * 	若末次支付日期为 null，则将首次支付用户数和支付独立用户数均置为 1；否则首次支付用户数置为 0，判断末次支付日期是否为当日，
 * 	如果不是当日则支付独立用户数置为 1，否则置为 0。最后将状态中的支付日期更新为当日。
 * 7）开窗、聚合
 * 度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * 8）写出到 ClickHouse
 */
public class TradePaymentSuccessWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从Kafka DWD层读取支付成功明细数据dwd_trade_pay_detail_suc 主体，并封装为流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);

        //TODO 3、过滤Null数据，并转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = dataStreamSource.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(value);
                        out.collect(jsonObject);
                    }
                }
        );

        //TODO 4、按照唯一键分组，使用的是支付ID
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(value -> value.getString("order_detail_id"));

        //TODO 5、使用状态编程去重，保留一条数据
        SingleOutputStreamOperator<JSONObject> filteredDS = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value", String.class);
                        StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        stateDescriptor.enableTimeToLive(stateTtlConfig);

                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String state = valueState.value();
                        if (state == null) {
                            valueState.update("1");
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
        );

        //TODO 6、按照uid进行分组
        KeyedStream<JSONObject, String> keyedByUserStream = filteredDS.keyBy(value -> value.getString("user_id"));

        //TODO 7、提取当日以及总的新增支付人数
        SingleOutputStreamOperator<TradePaymentWindow> tradePaymentDS = keyedByUserStream.flatMap(
                new RichFlatMapFunction<JSONObject, TradePaymentWindow>() {

                    private ValueState<String> lastPaymentDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastPaymentDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-payment", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradePaymentWindow> out) throws Exception {

                        //获取状态数据
                        String lastPayment = lastPaymentDataState.value();

                        //取出当前数据日期
                        String callbackTime = value.getString("callback_time");
                        String cutDt = callbackTime.split(" ")[0];

                        //定义两个数字
                        long paymentSuccessUniqueUserCount = 0L;
                        long paymentSuccessNewUserCount = 0L;

                        if (lastPayment == null) {//null代表为第一条数据，需要取用
                            paymentSuccessUniqueUserCount = 1L;
                            paymentSuccessNewUserCount = 1L;
                            lastPaymentDataState.update(cutDt);
                        } else if (!lastPayment.equals(cutDt)) {
                            paymentSuccessUniqueUserCount = 1L;
                            lastPaymentDataState.update(cutDt);
                        }

                        if (paymentSuccessUniqueUserCount == 1L) {
                            out.collect(new TradePaymentWindow(
                                            "",
                                            "",
                                            paymentSuccessUniqueUserCount,
                                            paymentSuccessNewUserCount,
                                            DateFormatUtil.toTs(callbackTime, true)
                                    )
                            );
                        }

                    }
                }
        );

        //TODO 8、提取事件时间、开窗、聚合
        SingleOutputStreamOperator<TradePaymentWindow> tradePaymentWithWaterMarkDS = tradePaymentDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradePaymentWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradePaymentWindow>() {
                                    @Override
                                    public long extractTimestamp(TradePaymentWindow element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 开窗
        AllWindowedStream<TradePaymentWindow, TimeWindow> windowedStream = tradePaymentWithWaterMarkDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 聚合
        SingleOutputStreamOperator<TradePaymentWindow> resultDS = windowedStream.reduce(
                new ReduceFunction<TradePaymentWindow>() {
                    @Override
                    public TradePaymentWindow reduce(TradePaymentWindow value1, TradePaymentWindow value2) throws Exception {
                        //TODO 如果使用的是带步长的滑动窗口，需要重新new一个对象来计算

                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());

                        return value1;
                    }
                },
                new AllWindowFunction<TradePaymentWindow, TradePaymentWindow, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindow> values, Collector<TradePaymentWindow> out) throws Exception {

                        TradePaymentWindow tradePaymentWindow = values.iterator().next();

                        tradePaymentWindow.setTs(System.currentTimeMillis());
                        tradePaymentWindow.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradePaymentWindow.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(tradePaymentWindow);

                    }
                }
        );

        //TODO 9、将数据写出到clickHouse
        resultDS.print("resultDS>>>>>>");
        resultDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_trade_payment_suc_window values(?,?,?,?,?)"
                )
        );

        env.execute("TradePaymentSucWindow");

    }

}
