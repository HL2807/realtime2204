package com.lqs.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.TradeOrder;
import com.lqs.utils.ClickHouseUtil;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import com.lqs.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
 * @Date 2022年04月24日 14:48:33
 * @Version 1.0.0
 * @ClassName TradeOrderWindow
 * @Describe 交易域下单各窗口汇总表
 * 需求：从 Kafka 订单明细主题读取数据，过滤 null 数据并去重，统计当日下单独立用户数和新增下单用户数，封装为实体类，
 * 写入 ClickHouse。
 * 思路步骤；
 * 我们在 DWD 层提到，订单明细表数据生成过程中会形成回撤流。left join 生成的数据集中，相同唯一键的数据可能会有多条。
 * 上文已有讲解，不再赘述。回撤数据在 Kafka 中以 null 值的形式存在，只需要简单判断即可过滤。我们需要考虑的是如何对其余数据去重。
 * 对回撤流数据生成过程进行分析，可以发现，字段内容完整数据的生成一定晚于不完整数据的生成，要确保统计结果的正确性，
 * 我们应保留字段内容最全的数据，基于以上论述，内容最全的数据生成时间最晚。要想通过时间筛选这部分数据，首先要获取数据生成时间。
 * <p>
 * （1）从 Kafka订单明细主题读取数据
 * （2）过滤为 null 数据并转换数据结构
 * 运用 filter 算子过滤为 null 的数据。
 * （3）按照 order_detail_id 分组
 * order_detail_id 为数据唯一键。
 * （4）对 order_detail_id 相同的数据去重
 * 按照上文提到的方案对数据去重。
 * （5）设置水位线
 * （6）按照用户 id 分组
 * （7）计算度量字段的值
 * a）当日下单独立用户数和新增下单用户数
 * 运用 Flink 状态编程，在状态中维护用户末次下单日期。
 * 若末次下单日期为 null，则将首次下单用户数和下单独立用户数均置为 1；否则首次下单用户数置为 0，判断末次下单日期是否为当日，
 * 如果不是当日则下单独立用户数置为 1，否则置为 0。最后将状态中的下单日期更新为当日。
 * b）其余度量字段直接取流中数据的对应值即可。
 * （8）开窗、聚合
 * 度量字段求和，补充窗口起始时间和结束时间字段，ts 字段置为当前系统时间戳。
 * （9）写出到 ClickHouse。
 */
public class TradeOrderWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取Kafka DWD层 订单明细主题数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";

        DataStreamSource<String> orderDetailStrDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3、过滤 “” 不需要的数据，保留“insert”数据，并将数据转换成JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = orderDetailStrDS.flatMap(
                new RichFlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        if (!"".equals(value)) {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if ("insert".equals(jsonObject.getString("type"))) {
                                out.collect(jsonObject);
                            }
                        }
                    }
                }
        );

        //TODO 4、按照order_detail_id进行数据分组
        KeyedStream<JSONObject, String> keyedByOrderDetailIdStream = jsonObjectDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("order_detail_id");
                    }
                }
        );

        //TODO 5、对老用户进行去重，通过对比时间进行数据中不是今日下单的用户进行去重
        SingleOutputStreamOperator<JSONObject> orderDetailJsonObjectDS = keyedByOrderDetailIdStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> orderDetailState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("order-detail", JSONObject.class);
                        orderDetailState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        //获取状态数据并判断是否有数据
                        JSONObject orderDetail = orderDetailState.value();

                        if (orderDetail == null) {
                            //把当前数据设置进状态并注册定时器
                            orderDetailState.update(value);
                            ctx.timerService().registerProcessingTimeTimer(
                                    ctx.timerService().currentProcessingTime() + 2000L
                            );
                        } else {
                            //2022-04-01 11:10:55.042Z
                            String stateTs = orderDetail.getString("ts");
                            //2022-04-01 11:10:55.9Z
                            String curTs = value.getString("ts");

                            int compare = TimestampLtz3CompareUtil.compare(stateTs, curTs);
                            if (compare != 1) {
                                //更新状态
                                orderDetailState.update(value);
                            }
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //提取状态数据并输出
                        JSONObject orderDetail = orderDetailState.value();
                        out.collect(orderDetail);
                    }
                }
        );

        //TODO 6、提取时间戳生成watermark
        SingleOutputStreamOperator<JSONObject> jsonObjectWithWatermarkDS = orderDetailJsonObjectDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return DateFormatUtil.toTs(element.getString("order_create_time"), true);
                                    }
                                }
                        )
        );

        //TODO 7、按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidStream = jsonObjectWithWatermarkDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("user_id");
                    }
                }
        );

        //TODO 8、提取下单的独立用户并转换为JavaBean对象
        SingleOutputStreamOperator<TradeOrder> tradeOrderDS = keyedByUidStream.flatMap(
                new RichFlatMapFunction<JSONObject, TradeOrder>() {

                    private ValueState<String> lastOrderDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastOrderDt = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-order", String.class));

                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradeOrder> out) throws Exception {
                        //取出状态时间
                        String lastOrder = lastOrderDt.value();

                        //取出当前数据的下单日期
                        String curDt = value.getString("order_create_time").split(" ")[0];

                        //定义独立下单数以及新增下单数
                        long orderUniqueUserCount = 0L;
                        long orderNewUserCount = 0L;

                        if (lastOrder == null) {
                            orderUniqueUserCount = 1L;
                            orderNewUserCount = 1L;

                            lastOrderDt.update(curDt);
                        } else if (!lastOrder.equals(curDt)) {
                            orderUniqueUserCount = 1L;
                            lastOrderDt.update(curDt);
                        }

                        //输出数据
                        Double activityReduceAmount = value.getDouble("activity_reduce_amount");
                        if (activityReduceAmount == null) {
                            activityReduceAmount = 0.00;
                        }

                        Double couponReduceAmount = value.getDouble("coupon_reduce_amount");
                        if (couponReduceAmount == null) {
                            couponReduceAmount = 0.00;
                        }

                        out.collect(
                                new TradeOrder(
                                        "",
                                        "",
                                        orderUniqueUserCount,
                                        orderNewUserCount,
                                        activityReduceAmount,
                                        couponReduceAmount,
                                        value.getDoubleValue("original_total_amount"),
                                        0L
                                )
                        );

                    }
                }
        );

        //TODO 9、开窗、聚合
        AllWindowedStream<TradeOrder, TimeWindow> windowedStream = tradeOrderDS.windowAll(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );
        SingleOutputStreamOperator<TradeOrder> resultDS = windowedStream.reduce(
                new ReduceFunction<TradeOrder>() {
                    @Override
                    public TradeOrder reduce(TradeOrder value1, TradeOrder value2) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + value2.getOrderActivityReduceAmount());
                        value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + value2.getOrderCouponReduceAmount());
                        value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + value2.getOrderOriginalTotalAmount());
                        return value1;
                    }
                },
                new AllWindowFunction<TradeOrder, TradeOrder, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrder> values, Collector<TradeOrder> out) throws Exception {

                        TradeOrder order = values.iterator().next();
                        order.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        order.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        order.setTs(System.currentTimeMillis());

                        out.collect(order);

                    }
                }
        );

        //TODO 10、将数据输出到ClickHouse
        resultDS.print("orderCount>>>>>>");
        resultDS.addSink(
                ClickHouseUtil.getClickHouseSink(
                        "insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"
                )
        );

        env.execute("TradeOrderWindow");

    }

}
