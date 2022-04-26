package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.CartAddUu;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月24日 14:25:43
 * @Version 1.0.0
 * @ClassName TradeCartAddUuWindow
 * @Describe 交易域加购各窗口汇总表
 * 需求：从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 ClickHouse。
 * 思路步骤：
 * 1、从 Kafka 加购明细主题读取数据
 * 2、转换数据结构
 * 	   将流中数据由 String 转换为 JSONObject。
 * 3、设置水位线
 * 4、按照用户 id 分组
 * 5、过滤独立用户加购记录
 * 	运用 Flink 状态编程，将用户末次加购日期维护到状态中。
 * 	如果末次登陆日期为 null 或者不等于当天日期，则保留数据并更新状态，否则丢弃，不做操作。
 * 6、开窗、聚合
 * 	统计窗口中数据条数即为加购独立用户数，补充窗口起始时间、关闭时间，将时间戳字段置为当前系统时间，发送到下游。
 * 7、将数据写入 ClickHouse。
 */
public class TradeCartAddUuWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从Kafka DWD层读取加购数据主题，创建流
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> cartAddStringDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3、将数据转换为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = cartAddStringDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                }
        );

        //TODO 4、提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjectWithWaterMarkDS = jsonObjectDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        //yyyy-MM-dd HH:mm:ss
                                        String dateTime = element.getString("operate_time");
                                        if (dateTime == null) {
                                            dateTime = element.getString("create_time");
                                        }
                                        return DateFormatUtil.toTs(dateTime, true);}
                                }
                        )
        );

        //TODO 5、按照 user_id 进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectWithWaterMarkDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("user_id");
                    }
                }
        );

        //TODO 6、过滤出独立用户，同时转换数据结构
        SingleOutputStreamOperator<CartAddUu> cartAddDS = keyedStream.flatMap(
                new RichFlatMapFunction<JSONObject, CartAddUu>() {

                    private ValueState<String> lastCartAddDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("cart-add", String.class);
                        StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        stateDescriptor.enableTimeToLive(stateTtlConfig);

                        lastCartAddDt = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<CartAddUu> out) throws Exception {
                        //获取状态数据
                        String lastDt = lastCartAddDt.value();
                        //yyyy-MM-dd HH:mm:ss
                        //yyyy-MM-dd HH:mm:ss
                        String dateTime = value.getString("operate_time");
                        if (dateTime == null) {
                            dateTime = value.getString("create_time");
                        }
                        String curDt = dateTime.split(" ")[0];

                        //如果状态数据为null或者与当前日期不是同一天，则保留塑化剂，更新状态
                        if (lastDt == null || !lastDt.equals(curDt)) {
                            lastCartAddDt.update(curDt);
                            out.collect(new CartAddUu("", "", 1L, 0L));
                        }
                    }
                }
        );

        //TODO 7、开窗、聚合
        AllWindowedStream<CartAddUu, TimeWindow> windowedStream = cartAddDS.windowAll(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        );

        SingleOutputStreamOperator<CartAddUu> resultDS = windowedStream.reduce(
                new ReduceFunction<CartAddUu>() {
                    @Override
                    public CartAddUu reduce(CartAddUu value1, CartAddUu value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                },
                new AllWindowFunction<CartAddUu, CartAddUu, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUu> values, Collector<CartAddUu> out) throws Exception {
                        //取出数据
                        CartAddUu cartAddUu = values.iterator().next();

                        //补充窗口信息
                        cartAddUu.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        cartAddUu.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        //补充版本信息
                        cartAddUu.setTs(System.currentTimeMillis());

                        //输出数据
                        out.collect(cartAddUu);
                    }
                }
        );

        //TODO 8、将数据写出到ClickHouse
        resultDS.print("CartCount>>>>>>");
        resultDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_trade_cart_add_uu_window values(?,?,?,?)"
                )
        );

        env.execute("TradeCartAddUuWindow");

    }

}
