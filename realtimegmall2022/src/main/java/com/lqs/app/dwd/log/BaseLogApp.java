package com.lqs.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lqs.utils.DateFormatUtil;
import com.lqs.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lqs
 * @Date 2022年04月13日 17:20:25
 * @Version 1.0.0
 * @ClassName BaseLogApp
 * @Describe 流量域未经加工的事务事实表
 * 1、数据清洗；数据传输过程中可能会出现部分数据丢失的情况，导致 JSON 数据结构不再完整，因此需要对脏数据进行过滤。
 * 2、新老访客状态标记修复：日志数据 common 字段下的 is_new 字段是用来标记新老访客状态的，1 表示新访客，2 表示老访客。
 * 前端埋点采集到的数据可靠性无法保证，可能会出现老访客被标记为新访客的问题，因此需要对该标记进行修复。
 * 3、分流；对日志数据进行拆分，生成五张事务事实表写入 Kafka
 * a、流量域页面浏览事务事实表
 * b、流量域启动事务事实表
 * c、流量域动作事务事实表
 * d、流量域曝光事务事实表
 * e、流量域错误事务事实表
 */
//数据流：web/app -> nginx -> 日志服务器(log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：  Mock -> f1.sh -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取Kafka topic_log主题数据并创建数据流
        String topic = "topic_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3、将数据转换为JSON格式，并过滤掉非JSON格式的数据
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };

        //TODO 处理脏数据，JSON数据主流输出，脏数据测流输出
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            ctx.output(dirtyTag, value);
                        }
                    }
                }
        );
        jsonObjectDS.getSideOutput(dirtyTag).print("Dirty>>>>>>");

        //TODO 4、使用状态编程来做新老用户的校验
        //TODO 将相同的key聚合起来
//        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjectDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        KeyedStream<JSONObject, String> keyedByMidDS = jsonObjectDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        JSONObject common = value.getJSONObject("common");
                        return common.getString("mid");
                    }
                }
        );
        //新老用户的校验
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlagDS = keyedByMidDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> lastVisitDataState;

                    //TODO 获取状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDataState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last-visit", String.class)
                        );
                    }

                    //TODO 新老用户校验
                    /*
                    1、如果 is_new 的值为 1
                        a、如果键控状态为 null，则将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        b、如果键控状态不为 null，且首次访问日期不是当日，则将 is_new 字段值为 0；
                        c、如果键控状态不为 null，且首次访问日期是当日，不做操作；
                    2、如果 is_new 的值为 0
                        a、如果键控状态为 null，则将状态中的首次访问日期更新为昨日。这样做可以保证同一mid的其它日志到来时，
                        依然会被判定为老访客；
                        b、如果键控状态不为 null，不做操作。
                     */
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {

                        // 1、通过获取“is_new”标记和获取状态数据
                        String isNew = value.getJSONObject("common").getString("is_new");
                        String lastVisitData = lastVisitDataState.value();
                        Long ts = value.getLong("ts");

                        //2、判断是否为“1”
                        if ("1".equals(isNew)) {

                            //3、获取当前数据的时间
                            String curDataTime = DateFormatUtil.toDate(ts);

                            if (lastVisitData == null) {
                                lastVisitDataState.update(curDataTime);
                            } else if (!lastVisitData.equals(curDataTime)) {
                                value.getJSONObject("common").put("is_new", "0");
                            }

                        } else if (lastVisitData == null) {
                            String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                            lastVisitDataState.update(yesterday);
                        }

                        return value;
                    }
                }
        );

        //TODO 5、使用侧输出流对数据进行分流处理
        /*
        a、流量域页面浏览事务事实表，输出到主流
        b、流量域启动事务事实表，输出到侧输出流
        c、流量域动作事务事实表，输出到侧输出流
        d、流量域曝光事务事实表，输出到侧输出流
        e、流量域错误事务事实表，输出到侧输出流
         */
        OutputTag<String> startTag = new OutputTag<String>("start"){
        };
        OutputTag<String> displayTag = new OutputTag<String>("display"){
        };
        OutputTag<String> actionTag = new OutputTag<String>("action"){
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        String jsonString = value.toJSONString();

                        //尝试获取出数据中的error字段
                        String error = value.getString("err");

                        if (error != null) {
                            //输出数据到错误日志
                            ctx.output(errorTag, jsonString);
                        }

                        //尝试获取启动字段
                        String start = value.getString("start");
                        if (start != null) {
                            //输出数据到启动日志
                            ctx.output(startTag, jsonString);
                        } else {

                            //取出页面id与时间戳
                            String pageId = value.getJSONObject("page").getString("page_id");
                            Long ts = value.getLong("ts");
                            String common = value.getString("common");

                            //尝试获取曝光数据
                            JSONArray displays = value.getJSONArray("displays");
                            if (displays != null && displays.size() > 0) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    display.put("page_id", pageId);
                                    display.put("ts", ts);
                                    display.put("common", common);

                                    ctx.output(displayTag, display.toJSONString());
                                }
                            }

                            //尝试获取动作数据
                            JSONArray actions = value.getJSONArray("actions");
                            if (actions != null && actions.size() > 0) {
                                for (int i = 0; i < actions.size(); i++) {
                                    JSONObject action = actions.getJSONObject(i);
                                    action.put("page_id", pageId);
                                    action.put("ts", ts);
                                    action.put("common", common);

                                    ctx.output(actionTag, action.toJSONString());
                                }
                            }

                            //输出数据到页面浏览日志
                            value.remove("displays");
                            value.remove("actions");
                            out.collect(value.toJSONString());

                        }
                    }
                }
        );

        //TODO 6、提取各个数据输出流中的数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        //TODO 7、将各个流的数据分别写出到对应的DWD层的Kafka主题当中

        pageDS.print("Page>>>>>>");
        startDS.print("Start>>>>>>");
        errorDS.print("Error>>>>>>");
        displayDS.print("Display>>>>>>");
        actionDS.print("Action>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(KafkaUtil.getKafkaProducer(page_topic));
        startDS.addSink(KafkaUtil.getKafkaProducer(start_topic));
        displayDS.addSink(KafkaUtil.getKafkaProducer(display_topic));
        actionDS.addSink(KafkaUtil.getKafkaProducer(action_topic));
        errorDS.addSink(KafkaUtil.getKafkaProducer(error_topic));

        env.execute("BaseLogApp");

    }

}
