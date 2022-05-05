package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.app.functions.DimAsyncFunction;
import com.lqs.bean.TradeTrademarkCategoryUserRefund;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author lqs
 * @Date 2022年04月27日 22:10:39
 * @Version 1.0.0
 * @ClassName TradeTrademarkCategoryUserRefundWindow
 * @Describe 交易域品牌-品类-用户粒度退单各窗口汇总表
 *
 * 需求：从 Kafka 读取退单明细数据，过滤 null 数据并按照唯一键对数据去重，关联维度信息，按照维度分组，
 * 统计各维度各窗口的订单数和订单金额，将数据写入 ClickHouse 交易域品牌-品类-用户粒度退单各窗口汇总表。
 *
 * 思路步骤：
 * 1）从 Kafka 退单明细主题读取数据
 * 2）过滤 null 数据并转换数据结构
 * 3）按照唯一键去重
 * 4）转换数据结构
 * 	JSONObject 转换为实体类 TradeTrademarkCategoryUserRefundBean。
 * 5）补充维度信息
 * 	（1）关联 sku_info 表
 * 	获取 tm_id，category3_id。
 * 	（2）关联 base_trademark 表
 * 	获取 tm_name。
 * 	（3）关联 base_category3 表
 * 	获取 name（三级品类名称），获取 category2_id。
 * 	（4）关联 base_categroy2 表
 * 	获取 name（二级品类名称），category1_id。
 * （5）关联 base_category1 表
 * 	获取 name（一级品类名称）。
 * 6）设置水位线
 * 7）分组、开窗、聚合
 * 按照维度信息分组，度量字段求和，并在窗口闭合后补充窗口起始时间和结束时间。将时间戳置为当前系统时间。
 * 8）写出到 ClickHouse。
 */
public class TradeTrademarkCategoryUserRefundWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从Kafka 退单明细主题读取数据，并封装为流
        String topic = "dwd_trade_order_refund";
        String groupId = "dwd_trade_order_refund_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> orderRefundDetailSource = env.addSource(kafkaConsumer);

//        orderRefundDetailSource.print();

        //TODO 3、过滤数据并转换为JSON对象
        SingleOutputStreamOperator<JSONObject> orderRefundDetailToJsonDS = orderRefundDetailSource.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        //TODO 注意，这里一定要进行判空，否则会报错
                        if (!"".equals(value)){
                            JSONObject jsonObject = JSON.parseObject(value);
                            out.collect(jsonObject);
                        }
                    }
                }
        );

        //TODO 4、按照id进行分组，使用退单order_id
        KeyedStream<JSONObject, String> keyedStream = orderRefundDetailToJsonDS.keyBy(value -> value.getString("order_id"));

        //TODO 5、使用状态编程去重，保留一条数据，
        SingleOutputStreamOperator<JSONObject> filteredDS = keyedStream.filter(
                new RichFilterFunction<JSONObject>() {

                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value-refund", String.class);
                        StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.seconds(2))
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

        //TODO 6、转换为实体类
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> ToRefundBeanDS = filteredDS.map(
                new MapFunction<JSONObject, TradeTrademarkCategoryUserRefund>() {
                    @Override
                    public TradeTrademarkCategoryUserRefund map(JSONObject value) throws Exception {
                        return TradeTrademarkCategoryUserRefund.builder()
                                .skuId(value.getString("sku_id"))
                                .userId(value.getString("user_id"))
                                .refundCount(1L)
                                .ts(DateFormatUtil.toTs(value.getString("create_time"),true))
                                .build();
                    }
                }
        );

        //TODO 7、关联维表
        //TODO 关联sku_info表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> withSkuDS = AsyncDataStream.unorderedWait(
                ToRefundBeanDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefund>("DIM_SKU_INFO") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefund input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefund input, JSONObject dimInfo) {
                        if (dimInfo != null) {
//                            System.out.println("CATEGORY3_ID:" + dimInfo.getString("CATEGORY3_ID"));
                            input.setTrademarkId(dimInfo.getString("TM_ID"));
                            input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 关联base_trademark表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> withTradeMarkDS = AsyncDataStream.unorderedWait(
                withSkuDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefund>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefund input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefund input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setTrademarkName(dimInfo.getString("TM_NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 关联base_category3表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> withCategory3DS = AsyncDataStream.unorderedWait(
                withTradeMarkDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefund>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefund input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefund input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory3Name(dimInfo.getString("NAME"));
                            input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 关联base_category2表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefund>("DIM_BASE_CATEGORY2") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefund input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefund input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory2Name(dimInfo.getString("NAME"));
                            input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 关联base_category1表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefund>("DIM_BASE_CATEGORY1") {

                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefund input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefund input, JSONObject dimInfo) {
                        if (dimInfo != null) {
                            input.setCategory1Name(dimInfo.getString("NAME"));
                        }
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        withCategory1DS.print("1>>>>>>>>>>>");

        //TODO 8、设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> tradeTrademarkCategoryUserRefundWaterMarkDS = withCategory1DS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeTrademarkCategoryUserRefund>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefund>() {
                                    @Override
                                    public long extractTimestamp(TradeTrademarkCategoryUserRefund element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 9、分组、开窗、聚合
        //TODO 分组
        KeyedStream<TradeTrademarkCategoryUserRefund, String> keyedStreamStream = tradeTrademarkCategoryUserRefundWaterMarkDS.keyBy(new KeySelector<TradeTrademarkCategoryUserRefund, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefund value) throws Exception {
                return  value.getSkuId() + "-" +
                        value.getTrademarkId() + "-" +
                        value.getTrademarkName() + "-" +
                        value.getCategory1Id() + "-" +
                        value.getCategory1Name() + "-" +
                        value.getCategory2Id() + "-" +
                        value.getCategory2Name() + "-" +
                        value.getCategory3Id() + "-" +
                        value.getCategory3Name();
            }
        });

        //TODO 开窗
        WindowedStream<TradeTrademarkCategoryUserRefund, String, TimeWindow> windowedStream = keyedStreamStream.window(
                TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))
        );

        //TODO 聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefund> resultUserRefundWindowDS = windowedStream.reduce(
                new ReduceFunction<TradeTrademarkCategoryUserRefund>() {
                    @Override
                    public TradeTrademarkCategoryUserRefund reduce(TradeTrademarkCategoryUserRefund value1, TradeTrademarkCategoryUserRefund value2) throws Exception {
                        value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());
                        return value1;
                    }
                },
                new WindowFunction<TradeTrademarkCategoryUserRefund, TradeTrademarkCategoryUserRefund, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefund> input, Collector<TradeTrademarkCategoryUserRefund> out) throws Exception {

                        TradeTrademarkCategoryUserRefund userRefund = input.iterator().next();

                        userRefund.setTs(System.currentTimeMillis());
                        userRefund.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userRefund.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                        out.collect(userRefund);

                    }
                }
        );

        //TODO 10、将数据写入到clickHouse
        resultUserRefundWindowDS.print("userRefundWindow>>>>>>");
        resultUserRefundWindowDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
                )
        );

        env.execute("TradeTrademarkCategoryUserRefundWindow");

    }

}
