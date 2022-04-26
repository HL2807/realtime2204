package com.lqs.app.functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.utils.KafkaUtil;
import com.lqs.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年04月24日 16:59:15
 * @Version 1.0.0
 * @ClassName OrderDetailFilterFunction
 * @Describe 分装订单明细去重返回函数
 */
public class OrderDetailFilterFunction {

    public static SingleOutputStreamOperator<JSONObject> getDwOrderDetail(StreamExecutionEnvironment environment,String groupId){
        String topic = "dwd_trade_order_detail";
        DataStreamSource<String> orderDetailStringDS = environment.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 1、过滤(""不需要,保留"insert")&转换数据为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = orderDetailStringDS.flatMap(
                new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        if (!"".equals(value)) {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if ("insert".equals(jsonObject.getString(value))) {
                                out.collect(jsonObject);
                            }
                        }
                    }
                }
        );

        //TODO 2、按照order_detail_id分组
        KeyedStream<JSONObject, String> keyedByOrderDetailIdStream = jsonObjectDS.keyBy(value -> value.getString("order_detail_id"));

        //TODO 3、将数据进行去重
        SingleOutputStreamOperator<JSONObject> filterResultDS = keyedByOrderDetailIdStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> orderDetailState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        orderDetailState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("order-detail", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

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
                            if (compare != 1) {  //表示后到的数据时间大
                                //更新状态
                                orderDetailState.update(value);
                            }
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject orderDetail = orderDetailState.value();
                        out.collect(orderDetail);
                    }
                }
        );

        return filterResultDS;

    }

}
