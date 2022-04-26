package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.TrafficPageView;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月22日 20:49:59
 * @Version 1.0.0
 * @ClassName TrafficVcChArIsNewPageViewWindow
 * @Describe 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 * 需求：DWS 层是为 ADS 层服务的，通过对指标体系的分析，本节汇总表中需要有会话数、页面浏览数、浏览总时长、独立访客数、
 * 跳出会话数五个度量字段。本节的任务是统计这五个指标，并将维度和度量数据写入 ClickHouse 汇总表。
 *
 * 思路步骤：
 * 任务可以分为两部分：统计指标的计算和数据写出，数据写出在10.1 节已有介绍，不再赘述。此处仅对统计指标计算进行分析。
 * 会话数、页面浏览数和浏览总时长三个指标均与页面浏览有关，可以由 DWD 层页面浏览明细表获得。独立访客数可以由 DWD
 * 层的独立访客明细表获得，跳出会话数可以由 DWD 层的用户跳出明细表获得。
 *
 * 三个主题读取的数据会在程序中被封装为三条流。处理后的数据要写入 ClickHouse 的同一张表，那么三条流的数据结构必须完全一致，
 * 这个问题很好解决，只要定义与表结构对应的实体类，然后将流中数据结构转换为实体类即可。除此之外，还有个问题需要考虑，三
 * 条流是否需要合并？ClickHouse 表的字段将按照窗口 + 表中所有维度做 order by，排序键是 ClickHouse 中的唯一键。
 * 如果三条流分别将数据写出到 ClickHouse，则对于唯一键相同的数据，不考虑重复写入的情况下会存在三条需要保留的数据
 * （度量数据分别存在于三条数据中）。我们使用了 ReplacingMergeTree，在分区合并时会按照排序键去重，排序字段相同的数据仅保留一条，
 * 将造成数据丢失。显然，这种方案是不可行的。此处将三条流合并为一条，对于每一排序键只生成一条数据。
 */
public class TrafficVcChArIsNewPageViewWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2、读取三个主题数据创建三个流
        String page_topic = "dwd_traffic_page_log";
        String uv_topic = "dwd_traffic_unique_visitor_detail";
        String uj_topic = "dwd_traffic_user_jump_detail";
        String groupId = "dws_traffic_vc_ch_ar_is_new_page_view_window7";

        DataStreamSource<String> pageStringDS = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupId));
        DataStreamSource<String> uvStringDS = env.addSource(KafkaUtil.getKafkaConsumer(uv_topic, groupId));
        DataStreamSource<String> ujStringDS = env.addSource(KafkaUtil.getKafkaConsumer(uj_topic, groupId));

        //TODO 3、将3个流统一数据格式 JavaBean
        //TODO 3.1、处理uv数据
        SingleOutputStreamOperator<TrafficPageView> trafficPageViewWithUvDS = uvStringDS.map(
                new MapFunction<String, TrafficPageView>() {
                    @Override
                    public TrafficPageView map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");
                        return new TrafficPageView(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                1L, 0L, 0L, 0L, 0L,
                                jsonObject.getLong("ts")
                        );
                    }
                }
        );

        //TODO 3.2、处理uj数据
        SingleOutputStreamOperator<TrafficPageView> trafficPageViewWithUjDS = ujStringDS.map(
                value -> {
                    JSONObject common = JSON.parseObject(value).getJSONObject("common");
                    return new TrafficPageView(
                            "",
                            "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 0L, 0L, 0L, 1L,
                            JSON.parseObject(value).getLong("ts")
                    );
                }
        );

        //TODO 3.3、处理page数据
        SingleOutputStreamOperator<TrafficPageView> trafficPageViewWithPvDS = pageStringDS.map(
                new MapFunction<String, TrafficPageView>() {
                    @Override
                    public TrafficPageView map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");

                        JSONObject page = jsonObject.getJSONObject("page");
                        String lastPageId = page.getString("last_page_id");

                        long sv = 0L;
                        if (lastPageId == null) {
                            sv = 1;
                        }

                        return new TrafficPageView(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                0L,
                                sv,
                                1L,
                                page.getLong("during_time"),
                                0L,
                                jsonObject.getLong("ts")
                        );

                    }
                }
        );

        //TODO 4、合并三个路并提取事件时间生成watermark
        SingleOutputStreamOperator<TrafficPageView> unionAndWaterMarkDS = trafficPageViewWithPvDS.union(
                trafficPageViewWithUjDS,
                trafficPageViewWithUvDS
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageView>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageView>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageView element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 5、分组、开窗、聚合
        //TODO 分组
        KeyedStream<TrafficPageView, Tuple4<String, String, String, String>> keyedStream = unionAndWaterMarkDS.keyBy(
                new KeySelector<TrafficPageView, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficPageView value) throws Exception {
                        return new Tuple4<>(
                                value.getAr(),//地区
                                value.getCh(),//渠道
                                value.getIsNew(),//新老访客状态标记
                                value.getVc()//app 版本号
                        );
                    }
                }
        );

        //TODO 开窗
        WindowedStream<TrafficPageView, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 聚合
        //优点：增量聚合  来一条聚合一条,效率高,存储空间占用少
//        windowedStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
//            @Override
//            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
//                return null;
//            }
//        });
        //优点：全量聚合  可以计算前百分比的结果、可以获取窗口信息
//        windowedStream.apply(new WindowFunction<TrafficPageViewBean, Object, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<Object> out) throws Exception {
//                window.getStart();
//                window.getEnd();
//            }
//        });
        SingleOutputStreamOperator<TrafficPageView> reduceDS = windowedStream.reduce(
                new ReduceFunction<TrafficPageView>() {
                    @Override
                    public TrafficPageView reduce(TrafficPageView value1, TrafficPageView value2) throws Exception {
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());//独立访客数
                        value1.setUjCt(value1.getUjCt() + value2.getUjCt());//跳出会话数
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());//累计访问时长
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());//页面浏览数
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());//会话数
                        return value1;
                    }
                },
                new WindowFunction<TrafficPageView, TrafficPageView, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageView> input, Collector<TrafficPageView> out) throws Exception {
                        //TODO 获取数据
                        TrafficPageView trafficPageView = input.iterator().next();

                        //获取窗口信息
                        long start = window.getStart();
                        long end = window.getEnd();

                        //补充窗口信息
                        trafficPageView.setStt(DateFormatUtil.toYmdHms(start));//窗口起始时间
                        trafficPageView.setEdt(DateFormatUtil.toYmdHms(end));

                        //输出数据
                        out.collect(trafficPageView);
                    }
                }
        );

        //TODO 6、将数据写出到clickHouse
        reduceDS.print("PageUvUj>>>>>>");
        reduceDS.addSink(ClickHouseUtil.getClickHouseSink(
                "insert into dws_traffic_channel_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute("TrafficVcChArIsNewPageViewWindow");

    }

}
