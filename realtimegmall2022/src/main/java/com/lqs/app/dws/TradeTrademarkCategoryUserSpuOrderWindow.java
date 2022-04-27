package com.lqs.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.lqs.app.functions.DimAsyncFunction;
import com.lqs.app.functions.OrderDetailFilterFunction;
import com.lqs.bean.TradeTrademarkCategoryUserSpuOrder;
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
 * @Date 2022年04月24日 16:56:17
 * @Version 1.0.0
 * @ClassName TradeTrademarkCategoryUserSpuOrderWindow
 * @Describe 交易域品牌-品类-用户-SPU粒度下单各窗口汇总表
 * <p>
 * 需求：从 Kafka 订单明细主题读取数据，过滤 null 数据并按照唯一键对数据去重，关联维度信息，按照维度分组，
 * 统计各维度各窗口的订单数和订单金额，将数据写入 ClickHouse 交易域品牌-品类-用户-SPU粒度下单各窗口汇总表。
 * <p>
 * 思路步骤：
 * （1）从 Kafka 订单明细主题读取数据
 * （2）过滤 null 数据并转换数据结构
 * （3）按照唯一键去重
 * （4）转换数据结构
 * JSONObject 转换为实体类 TradeTrademarkCategoryUserSpuOrderBean。
 * （5）补充维度信息
 * ① 关联 sku_info 表
 * 获取 tm_id，category3_id，spu_id。
 * ② 关联 spu_info 表
 * 获取 spu_name。
 * ③ 关联 base_trademark 表
 * 获取 tm_name。
 * ④ 关联 base_category3 表
 * 获取 name（三级品类名称），获取 category2_id。
 * ⑤ 关联 base_categroy2 表
 * 获取 name（二级品类名称），category1_id。
 * ⑥ 关联 base_category1 表
 * 获取 name（一级品类名称）。
 * （6）设置水位线
 * （7）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * （8）写出到 ClickHouse。
 */
public class TradeTrademarkCategoryUserSpuOrderWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 获取过滤后的数据
        String groupId = "sku_user_order_window";
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjectDS = OrderDetailFilterFunction.getDwOrderDetail(env, groupId);

//        orderDetailJsonObjectDS.print("test>>>>>>>>>>");
        //TODO 3、将数据转换为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> skuUserOrderDS = orderDetailJsonObjectDS.map(
                new MapFunction<JSONObject, TradeTrademarkCategoryUserSpuOrder>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrder map(JSONObject value) throws Exception {
                        return TradeTrademarkCategoryUserSpuOrder.builder()
                                .skuId(value.getString("sku_id"))
                                .userId(value.getString("user_id"))
                                .orderCount(1L)
                                .orderAmount(value.getDouble("split_total_amount"))
                                .ts(value.getLong("order_create_time"))
                                .build();
                    }
                }
        );

        //TODO 4、关联维表
/*
        skuUserOrderDS.map(new RichMapFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                //创建连接
            }
            @Override
            public TradeTrademarkCategoryUserSpuOrderBean map(TradeTrademarkCategoryUserSpuOrderBean value) throws Exception {
                //查询SKU表
                //DimUtil.getDimInfo(conn, "", value.getSkuId());
                //查询SPU表
                //... ...
                return value;
            }
        });
        skuUserOrderDS.print("skuUserOrderDS>>>>");
*/
        //TODO 4.1、关联sku表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> withSkuDS = AsyncDataStream.unorderedWait(
                skuUserOrderDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrder>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrder input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrder input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setSpuId(dimInfo.getString("SPU_ID"));
                            input.setTrademarkId(dimInfo.getString("TM_ID"));
                            input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 4.2、关联spu表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> withSpuDS = AsyncDataStream.unorderedWait(
                withSkuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrder>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrder input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrder input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setSpuName(dimInfo.getString("SPU_NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 4.3、关联Trademark表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> withTradMarkDS = AsyncDataStream.unorderedWait(
                withSpuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrder>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrder input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrder input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setTrademarkName(dimInfo.getString("TM_NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TOOD 4.4、关联Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> withCategory3DS = AsyncDataStream.unorderedWait(
                withTradMarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrder>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrder input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrder input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory3Name(dimInfo.getString("NAME"));
                            input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                        }
                    }

                },
                60, TimeUnit.SECONDS
        );

        //TOOD 4.5、关联Category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrder>("DIM_BASE_CATEGORY2") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrder input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrder input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory2Name(dimInfo.getString("NAME"));
                            input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                        }
                    }

                },
                60, TimeUnit.SECONDS
        );

        //TOOD 4.6、关联Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserSpuOrder>("DIM_BASE_CATEGORY1") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrder input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrder input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory1Name(dimInfo.getString("NAME"));
                        }
                    }

                },
                60, TimeUnit.SECONDS
        );

        //打印测试
//        withCategory1DS.print("withCategory1DS>>>>>>>>");

        //TODO 5.提取时间戳生成WaterMark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> userSpuOrderWithWatermark = withCategory1DS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserSpuOrder>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrder>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserSpuOrder element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 6.分组、开窗聚合
        KeyedStream<TradeTrademarkCategoryUserSpuOrder, String> keyedStream = userSpuOrderWithWatermark.keyBy(
                new KeySelector<TradeTrademarkCategoryUserSpuOrder, String>() {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrder value) throws Exception {
                        return value.getUserId() + "-" +
                                value.getCategory1Id() + "-" +
                                value.getCategory1Name() + "-" +
                                value.getCategory2Id() + "-" +
                                value.getCategory2Name() + "-" +
                                value.getCategory3Id() + "-" +
                                value.getCategory3Name() + "-" +
                                value.getSpuId() + "-" +
                                value.getSpuName() + "-" +
                                value.getTrademarkId() + "-" +
                                value.getTrademarkName();
                    }
                }
        );

        //TODO 开窗
        WindowedStream<TradeTrademarkCategoryUserSpuOrder, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrder> resultDS = windowedStream.reduce(
                new ReduceFunction<TradeTrademarkCategoryUserSpuOrder>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrder reduce(TradeTrademarkCategoryUserSpuOrder value1, TradeTrademarkCategoryUserSpuOrder value2) throws Exception {

                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;

                    }
                },
                new WindowFunction<TradeTrademarkCategoryUserSpuOrder, TradeTrademarkCategoryUserSpuOrder, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserSpuOrder> input, Collector<TradeTrademarkCategoryUserSpuOrder> out) throws Exception {

                        TradeTrademarkCategoryUserSpuOrder orderNext = input.iterator().next();

                        orderNext.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        orderNext.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(orderNext);

                    }
                }
        );

        //TODO 7.将数据写出到ClickHouse
        resultDS.print("resultDS>>>>>>");
        resultDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_trade_trademark_category_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                )
        );

        //TODO 8.启动任务
        env.execute("TradeTrademarkCategoryUserSpuOrderWindow");

    }

}
