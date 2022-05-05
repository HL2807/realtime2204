package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.TrafficHomeDetailPageView;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月23日 11:27:01
 * @Version 1.0.0
 * @ClassName TrafficPageViewWindow
 * @Describe 流量域页面浏览各窗口汇总表
 * 需求：
 * 从 Kafka 页面日志主题读取数据，统计当日的首页和商品详情页独立访客数。
 *
 * 思路步骤：
 * 1、读取 Kafka 页面主题数据
 * 2、转换数据结构
 *  将流中数据由 String 转换为 JSONObject。
 * 3、过滤数据
 * 	仅保留 page_id 为 home 或 good_detail 的数据，因为本程序统计的度量仅与这两个页面有关，其它数据无用。
 * 4、设置水位线
 * 5、按照 mid 分组
 * 6、统计首页和商品详情页独立访客数，转换数据结构
 *  运用 Flink 状态编程，为每个 mid 维护首页和商品详情页末次访问日期。如果 page_id 为 home，当状态中存储的日期为 null 或不是当日时，将 homeUvCt（首页独立访客数） 置为 1，并将状态中的日期更新为当日。否则置为 0，不做操作。商品详情页独立访客的统计同理。将统计结果和相关维度信息封装到定义的实体类中，发送到下游。
 * 7、开窗
 * 8、聚合
 * 9、将数据写出到 ClickHouse
 */
public class TrafficPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、读取Kafka页面日志主题数据并创建数据流
        String page_topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
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
        //简写版本
//        SingleOutputStreamOperator<JSONObject> jsonObjectDS = pageStringDS.map(JSON::parseObject);

        //TODO 4、过滤数据，只需要访问主页和商品详细页的数据
        SingleOutputStreamOperator<JSONObject> homeAndDetailPageDS = jsonObjectDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String pageID = value.getJSONObject("page").getString("page_id");
                        return "good_detail".equals(pageID) || "home".equals(pageID);
                    }
                }
        );

        //TODO 对3和4实现的功能进行简写
        SingleOutputStreamOperator<JSONObject> flatMapDS = pageStringDS.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        if ("good_detail".equals(pageId) || "home".equals(pageId)) {
                            out.collect(jsonObject);
                        }
                    }
                }
        );

        //TODO 5、提取事件时间生成watermark
        SingleOutputStreamOperator<JSONObject> homeAndDetailPageWithDS = homeAndDetailPageDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );

        //TODO 6、按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = homeAndDetailPageWithDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //TODO 7、使用状态编程计算主页和商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageView> trafficHomeDetailDS = keyedStream.flatMap(

                new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageView>() {

                    private ValueState<String> homeLastVisitDt;
                    private ValueState<String> detailLastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //设置状态TTL
                        StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();

                        //初始化Home页面访问日期状态
                        ValueStateDescriptor<String> homeDtDescriptor = new ValueStateDescriptor<>("home-dt", String.class);
                        homeDtDescriptor.enableTimeToLive(stateTtlConfig);
                        homeLastVisitDt = getRuntimeContext().getState(homeDtDescriptor);

                        //初始化商品详情页访问日期状态
                        ValueStateDescriptor<String> detailDtDescriptor = new ValueStateDescriptor<>("detail-dt", String.class);
                        detailDtDescriptor.enableTimeToLive(stateTtlConfig);
                        detailLastVisitDt = getRuntimeContext().getState(detailDtDescriptor);
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageView> out) throws Exception {
                        //取出当前页面信息和时间戳并将其转为日期
                        String pageId = value.getJSONObject("page").getString("page_id");
                        Long ts = value.getLong("ts");
                        String curDt = DateFormatUtil.toDate(ts);

                        //定义主页以及商品详情页的访问次数
                        long homeUvCt = 0L;
                        long detailUvCt = 0L;

                        //判断是否为主页数据
                        if ("home".equals(pageId)) {
                            //判断状态以及当前日期是否相同
                            String homeLastDt = homeLastVisitDt.value();
                            if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                                homeUvCt = 1L;
                                homeLastVisitDt.update(curDt);
                            }
                        } else {
                            //商品详情页
                            //判断状态以及与当前日期是否相同
                            String detailLastDt = detailLastVisitDt.value();
                            if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                                detailUvCt = 1L;
                                detailLastVisitDt.update(curDt);
                            }
                        }

                        //封装JavaBean并写出数据
                        if (homeUvCt != 0L || detailUvCt != 0L) {
                            out.collect(new TrafficHomeDetailPageView("", "",
                                    homeUvCt,
                                    detailUvCt,
                                    System.currentTimeMillis()));
                        }

                    }
                }
        );

        //TODO 8、开窗聚合
        AllWindowedStream<TrafficHomeDetailPageView, TimeWindow> windowedStream = trafficHomeDetailDS.windowAll(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        );

        SingleOutputStreamOperator<TrafficHomeDetailPageView> reduceDS = windowedStream.reduce(
                new ReduceFunction<TrafficHomeDetailPageView>() {
                    @Override
                    public TrafficHomeDetailPageView reduce(TrafficHomeDetailPageView value1, TrafficHomeDetailPageView value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageView, TrafficHomeDetailPageView, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageView> values, Collector<TrafficHomeDetailPageView> out) throws Exception {
                        //获取数据
                        TrafficHomeDetailPageView pageViewBean = values.iterator().next();

                        //设置窗口信息
                        pageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        pageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        //输出数据
                        out.collect(pageViewBean);
                    }
                }
        );

        //TODO 9、将数据输出到clickHouse
        reduceDS.print("uvCount>>>>>>");
        reduceDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                       "insert into dws_traffic_page_view_window values(?,?,?,?,?)"
                )
        );

        env.execute("TrafficPageViewWindow");

    }

}
