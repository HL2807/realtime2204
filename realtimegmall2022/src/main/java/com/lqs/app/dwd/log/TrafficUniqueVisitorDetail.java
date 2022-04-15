package com.lqs.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月15日 11:35:50
 * @Version 1.0.0
 * @ClassName TrafficUniqueVisitorDetail
 * @Describe 用户日活计算（uv）
 * 整体思路：
 *      1、过滤last_page_id不为null的数据，可以减少数据的计算
 *      2、筛选独立访客记录。运用Flink状态编程，为每个mid维护一个键控状态，记录末次登录时间。
 *          如果末次登录日期为null或者不是今天，则此mid为首次访问，保留当前数据，将末次登录日期更新为当然，否则不是当日并丢弃数据。
 *      3、状态存活时间设置
 *          设置状态的 TTL 为 1 天，更新模式为 OnCreateAndWrite，表示在创建和更新状态时重置状态存活时间。
 *          列如：2022-02-21 08:00:00 首次访问，若 2022-02-22 没有访问记录，则 2022-02-22 08:00:00 之后状态清空。
 */
public class TrafficUniqueVisitorDetail {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取Kafka dwd_traffic_page_log（BaseLogApp）主题数据并创建数据流。
        String sourceTopic = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_unique_visitor_detail";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3、将每行数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                }
        );
        //简写
//        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(JSON::parseObject);

        //TODO 4、过滤掉上一跳（last_page_id）页面id不等于null的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjectDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                }
        );

        //TODO 5、按照键控状态对mid进行分组
        KeyedStream<JSONObject, String> keyedByMidDS= filterDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                }
        );
        //简写版本
//        KeyedStream<JSONObject, String> keyedByMidStream = filterDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //TODO 6、使用状态编程对每日登录的数据进行时间重复的去重
        SingleOutputStreamOperator<JSONObject> uvDetailDS = keyedByMidDS.filter(
                new RichFilterFunction<JSONObject>() {

                    private ValueState<String> visitDataStreamState;

                    //获取数据状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("visit-dt", String.class);
                        //设置状态TTL，解决数据保留问题，即只把状态保留24小时
                        StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                        //初始化状态
                        visitDataStreamState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //取出状态以及当前数据的日期
                        String dt = visitDataStreamState.value();
                        String curDataTime = DateFormatUtil.toDate(value.getLong("ts"));

                        //如果状态数据为null或者状态日期与当前数据日期不同，保留数据，同时更新状态，反之则舍弃
                        if (dt == null || !dt.equals(curDataTime)) {
                            visitDataStreamState.update(curDataTime);
                            return true;
                        }
//                        else {
//                            return false;
//                        }

                        return false;
                    }
                }
        );

        //TODO 7、将数据写出到Kafka
        uvDetailDS.print("UV>>>>>>");
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        SingleOutputStreamOperator<String> uvResultDS = uvDetailDS.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject value) throws Exception {
                        return value.toJSONString();
                    }
                }
        );

        uvResultDS.addSink(KafkaUtil.getKafkaProducer(targetTopic));

        //简写
//        uvDetailDS.map(JSONAware::toJSONString).
//                addSink(KafkaUtil.getKafkaProducer(targetTopic));

        env.execute("TrafficUniqueVisitorDetail");

    }


}