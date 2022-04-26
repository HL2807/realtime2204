package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.UserLogin;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月23日 20:52:49
 * @Version 1.0.0
 * @ClassName UserUserLoginWindow
 * @Describe 用户域用户登录各窗口汇总表
 * 需求：从 Kafka 页面日志主题读取数据，统计七日回流用户和当日独立用户数。
 * 思路步骤：
 * 之前的活跃用户，一段时间未活跃（流失），今日又活跃了，就称为回流用户。此处要求统计回流用户总数。
 * 规定当日登陆，且自上次登陆之后至少 7 日未登录的用户为回流用户。
 */
public class UserUserLoginWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取Kafka页面日志主题数据创建流
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_user_user_login_window_211027";
        DataStreamSource<String> pageStringDS = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupId));

        //TODO 3、将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = pageStringDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                }
        );

        //TODO 4、过滤数据 uid不为null and last_page_id==null
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjectDS.filter(
                new RichFilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        JSONObject common = value.getJSONObject("common");
                        JSONObject page = value.getJSONObject("page");
                        return common.getString("uid") != null && page.getString("last_page_id") == null;
                    }
                }
        );

        //TODO 简写4、3
        SingleOutputStreamOperator<JSONObject> flatMapDS = pageStringDS.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");
                        JSONObject page = jsonObject.getJSONObject("page");
                        if (common.getString("uid") != null && page.getString("last_page_id") == null) {
                            out.collect(jsonObject);
                        }
                    }
                }
        );

        //TODO 5、提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> flatMapWithWaterMarkDS = flatMapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );

        //TODO 6、按照uid分组
        KeyedStream<JSONObject, String> keyedStream = flatMapWithWaterMarkDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("uid");
                    }
                }
        );

        //TODO 7、使用状态编程实现回流和独立用户的提取
        SingleOutputStreamOperator<UserLogin> uvDS = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, UserLogin>() {

                    private ValueState<String> lastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last-dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLogin>.Context ctx, Collector<UserLogin> out) throws Exception {

                        //取出状态中的数据，即为上一次用户登录的保存日期
                        String lastDt = lastVisitDt.value();

                        //获取当前数据中的时间并转换为日期
                        Long ts = value.getLong("ts");
                        String curDt = DateFormatUtil.toDate(ts);

                        //定义独立用户和回流用户的个数，默认为0个
                        long uuCt = 0L;
                        long backCt = 0L;

                        //状态保存的日期为null，则表示为新用户
                        if (lastDt == null) {
                            uuCt = 1l;
                            lastVisitDt.update(curDt);
                        } else {
                            //状态保存的日期不为null，且与当前数据日期不同，则为今天第一条数据
                            if (!lastDt.equals(curDt)) {
                                uuCt = 1L;
                                lastVisitDt.update(curDt);

                                //如果保存的日期与当前数据日期差值大于等于8days，则为回流用户
                                Long lastTs = DateFormatUtil.toTs(lastDt);
                                long days = (ts - lastTs) / (1000L * 60 * 60 * 24);
                                if (days >= 8L) {
                                    backCt = 1;
                                }
                            }
                        }

                        //判断，当独立用户数为1是时，则输出
                        if (uuCt == 1L) {
                            out.collect(
                                    new UserLogin(
                                            "",
                                            "",
                                            backCt,
                                            uuCt,
                                            System.currentTimeMillis()
                                    )
                            );
                        }

                    }
                }
        );

        //TODO 8、开窗、聚合
        AllWindowedStream<UserLogin, TimeWindow> windowedStream = uvDS.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        SingleOutputStreamOperator<UserLogin> resultDS = windowedStream.reduce(
                new ReduceFunction<UserLogin>() {
                    @Override
                    public UserLogin reduce(UserLogin value1, UserLogin value2) throws Exception {
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLogin, UserLogin, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLogin> values, Collector<UserLogin> out) throws Exception {

                        //取出数据
                        UserLogin userLogin = values.iterator().next();

                        //补充窗口信息
                        userLogin.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userLogin.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        //输出数据
                        out.collect(userLogin);

                    }
                }
        );

        //TODO 9、将数据写出到ClickHouse
        resultDS.print("userLoginCount>>>>>>");
        resultDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_user_user_login_window values(?,?,?,?,?)"
                )
        );

        env.execute("UserUserLoginWindow");

    }

}
