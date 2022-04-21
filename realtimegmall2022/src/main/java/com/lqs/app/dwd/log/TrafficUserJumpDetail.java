package com.lqs.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lqs
 * @Date 2022年04月15日 17:53:37
 * @Version 1.0.0
 * @ClassName TrafficUserJumpDetail
 * @Describe 流量域用户跳出明细
 * 需求：
 *      结合访问时间、page_id、last_page_id字段对整体数据集做处理，只要筛选页面数为1的页面即为跳出明细页面
 * 实现思路：
 *      1、对数据按照mid分组。因为访客的浏览记录互补干涉，跳出行为的分析应该在相同mid下进行，为此先按照mid进行分组
 *      2、定义CEP匹配规则
 *          a、规则一：跳出行为对应的页面日志为某一会话的首页，因此第一个规则判定last_page_id是否为null，是则返回true，反之false
 *          b、规则二：规则二和规则一之间的策略采用严格连续，并要求他们之间不能有其他事件。
 *              首先判定last_page_id是否为null，在数据完整有序的前提下，如果不是null说明本条日志的页面不是首页，可以断定它与
 *              规则一匹配到的事件属于同一个会话，返回true，反之则开启一个新会话并返回false
 *          c、超时时间
 *              超时时间内规则一满足，规则二没有满足就会被判定为超时数据。
 *      3、把匹配规则（pattern）应用到流上，根据pattern定义的规则对流中数据进行筛选。
 *      4、提取超时数据流
 *          把满足超时数据流中规则一的数据取出即为跳出明细数据，并将这个数据写出到Kafka跳出明细主题
 */

//数据流：web/app -> Nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//程  序：  Mock -> f1.sh -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)

public class TrafficUserJumpDetail {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取Kafka dwd_traffic_page_log主题数据，并创建数据流
        String sourceTopic  = "dwd_traffic_page_log";
        String groupId = "dwd_traffic_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3、将数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                }
        );
        //简写版
//        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(JSON::parseObject);

        //TODO 4、提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjectWithWaterMarkDS = jsonObjectDS.assignTimestampsAndWatermarks(
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

        //TODO 5、按照mid进行分组
        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjectWithWaterMarkDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getJSONObject("common").getString("mid");
                    }
                }
        );
        //简写版本
//        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjectWithWaterMarkDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //TODO 6、定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) throws Exception {
                                return value.getJSONObject("page").getString("last_page_id") == null;
                            }
                        }
                )
                .next("second")
                .where(
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) throws Exception {
                                return value.getJSONObject("page").getString("last_page_id") == null;
                            }
                        }
                )
                //设置超时时间为10s
                .within(Time.seconds(10));
        //写法2
       /* Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                //默认为宽松近邻
                .times(2)
                //根据前面的分析，这里需要使用严格近邻
                .consecutive()
                .within(Time.seconds(10));*/

        //TODO 7、将定义的模式序列作用到分组后的流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedByMidDS, pattern);

        //TODO 8、提取匹配上的事件数据以及超时事件的数据流
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeout") {
        };
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(
                timeoutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.get("first").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("first").get(0);
                    }
                }
        );
        //TODO 提取超时侧输出流
        DataStream<JSONObject> timeoutDS = selectDS.getSideOutput(timeoutTag);

        //TODO 9、合并两个事件流
        selectDS.print("Select>>>>>>");
        timeoutDS.print("Timeout>>>>>>");
        DataStream<JSONObject> unionDS = selectDS.union(timeoutDS);

        //TODO 10、将数据写出到Kafka主题dwd_traffic_user_jump_detail
        String targetTopic = "dwd_traffic_user_jump_detail";
        SingleOutputStreamOperator<String> resultDS = unionDS.map(
                new MapFunction<JSONObject, String>() {
                    @Override
                    public String map(JSONObject value) throws Exception {
                        return value.toJSONString();
                    }
                }
        );

        resultDS.addSink(KafkaUtil.getKafkaProducer(targetTopic));

        env.execute("TrafficUserJumpDetail");

    }

}




