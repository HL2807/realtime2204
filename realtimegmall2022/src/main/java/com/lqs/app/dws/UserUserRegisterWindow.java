package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.UserRegister;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月23日 16:47:11
 * @Version 1.0.0
 * @ClassName UserUserRegisterWindow
 * @Describe 用户域用户登录各窗口汇总表
 * 需求：从 Kafka 页面日志主题读取数据，统计七日回流用户和当日独立用户数
 * 思路步骤：
 * 之前的活跃用户，一段时间未活跃（流失），今日又活跃了，就称为回流用户。此处要求统计回流用户总数。规定当日登陆，且自上次登陆之后至少 7 日未登录的用户为回流用户。
 */
public class UserUserRegisterWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取用户注册主题数据
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window_211027";
        DataStreamSource<String> registerDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3、将每行数据转换为JavaBean对象,并提取时间戳生成watermark
        SingleOutputStreamOperator<UserRegister> userRegisterWaterMarkDS = registerDS.map(
                new MapFunction<String, UserRegister>() {
                    @Override
                    public UserRegister map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        return new UserRegister(
                                "",
                                "",
                                1L,
                                jsonObject.getLong("ts") * 1000L
                        );
                    }
                }
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<UserRegister>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<UserRegister>() {
                                    @Override
                                    public long extractTimestamp(UserRegister element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 4、开窗、聚合
        AllWindowedStream<UserRegister, TimeWindow> windowedStream = userRegisterWaterMarkDS.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );
        SingleOutputStreamOperator<UserRegister> resultDS = windowedStream.reduce(
                new ReduceFunction<UserRegister>() {
                    @Override
                    public UserRegister reduce(UserRegister value1, UserRegister value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserRegister, UserRegister, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegister> values, Collector<UserRegister> out) throws Exception {
                        //读取数据
                        UserRegister userRegister = values.iterator().next();
                        //补充窗口信息
                        userRegister.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userRegister.setEdt(DateFormatUtil.toYmdHms(window.getStart()));
                        //输出数据
                        out.collect(userRegister);
                    }
                }
        );

        //TODO 5、将数据写出到ClickHouse
        registerDS.print("userRegisterCount>>>>>>");
        registerDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_user_user_register_window values(?,?,?,?)"
                )
        );

        env.execute("UserUserRegisterWindow");

    }

}
