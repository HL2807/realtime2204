package com.lqs.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.app.functions.DimSinkFunction;
import com.lqs.app.functions.TableProcessFunction;
import com.lqs.bean.TableProcess;
import com.lqs.utils.KafkaUtil;
import com.sun.xml.internal.bind.v2.TODO;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

/**
 * @Author lqs
 * @Date 2022年04月11日 21:43:16
 * @Version 1.0.0
 * @ClassName DimSink
 * @Describe 接收Kafka数据，过滤空值数据
 */

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(Binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Phoenix(DIM)
//程  序：    Mock -> Mysql(Binlog) -> maxwell.sh -> Kafka(ZK) -> DimApp -> Phoenix(HBase HDFS/ZK)

public class DimSinkApp {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、设置状态后端
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                10,
//                Time.of(1L, TimeUnit.DAYS),
//                Time.of(3L,TimeUnit.MINUTES)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://nwh120:9020/flink_realtime/ck");
//        System.setProperty("HADOOP_USER_NAME","lqs");

        //TODO 2.读取 Kafka topic_dbrt 主题数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer("topic_db", "dim_app_sink"));

        //TODO 3.过滤掉非JSON格式的数据,并将其写入侧输出流
        OutputTag<String> dirtyDataTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyDataTag, s);
                }
            }
        });

        //取出脏数据并打印
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyDataTag);
        sideOutput.print("Dirty>>>>>>>>>>");

        //TODO 4.使用FlinkCDC读取MySQL中的配置信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("nwh120")
                .port(3306)
                .username("root")
                .password("912811")
                .databaseList("gmall2204_config")
                .tableList("gmall2204_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                //earliest:需要在建库之前就开启Binlog,也就是说Binlog中需要有建库建表语句
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        //TODO 5.将配置信息流处理成广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流与广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        //TODO 7.根据广播流数据处理主流数据
        SingleOutputStreamOperator<JSONObject> hbaseDS = connectedStream.process(new TableProcessFunction(mapStateDescriptor));

        //TODO 8.将数据写出到Phoenix中
        hbaseDS.print("hbase>>>>>>");
        hbaseDS.addSink(new DimSinkFunction());

        /*
        运行结果
        hbase>>>>>>> {"sinkTable":"dim_base_trademark","database":"gmall2204","xid":28107,"data":{"tm_name":"lqs","id":23},"commit":true,"type":"insert","table":"base_trademark","ts":1649705398}
        Phoenix sql语句upsert into GMALL2204_REALTIME.dim_base_trademark(tm_name,id) values ('lqs','23')
        hbase>>>>>>> {"sinkTable":"dim_base_trademark","database":"gmall2204","xid":28224,"data":{"tm_name":"lqs","id":12},"old":{"id":23},"commit":true,"type":"update","table":"base_trademark","ts":1649705449}
        Phoenix sql语句upsert into GMALL2204_REALTIME.dim_base_trademark(tm_name,id) values ('lqs','12')
        hbase>>>>>>> {"sinkTable":"dim_base_trademark","database":"gmall2204","xid":28269,"data":{"tm_name":"hll","id":12},"old":{"tm_name":"lqs"},"commit":true,"type":"update","table":"base_trademark","ts":1649705468}
        Phoenix sql语句upsert into GMALL2204_REALTIME.dim_base_trademark(tm_name,id) values ('hll','12')
        hbase>>>>>>> {"sinkTable":"dim_base_category1","database":"gmall2204","xid":28373,"data":{"name":"大炮飞机","id":18},"commit":true,"type":"insert","table":"base_category1","ts":1649705509}
        Phoenix sql语句upsert into GMALL2204_REALTIME.dim_base_category1(name,id) values ('大炮飞机','18')
        过滤掉：{"database":"gmall2204","xid":28502,"data":{"name":"大炮飞机","id":18},"commit":true,"type":"delete","table":"base_category1","ts":1649705565}
        过滤掉：{"database":"gmall2204","xid":28534,"data":{"tm_name":"hll","logo_url":"test","id":12},"commit":true,"type":"delete","table":"base_trademark","ts":1649705578}

         */

        //TODO 9.启动任务
        env.execute("DimSinkApp");

       /* //TODO 4、主流数据结构转换
        SingleOutputStreamOperator<JSONObject> jsonDS = dbDS.map(JSON::parseObject);

        //TODO 5、主流ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        value.getJSONObject("data");
                        if (value.getString("type").equals("bootstrap-complete")) {
                            return false;
                        }
                        return true;

                    }
                }
        );

        filterDS.print("filterDS>>>>>>");

        //TODO 6、FlinkCDC读取配置流并广播
        //TODO 6.1 读取配置表信息
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("nwh120")
                .port(3306)
                .username("root")
                .password("912811")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                //earliest:需要在建库之前就开启Binlog,也就是说Binlog中需要有建库建表语句
                .startupOptions(StartupOptions.initial())
                .build();

        //TODO 6.2 封装为流
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");

        //TODO 6.3广播配置流
        MapStateDescriptor<String, TableProcess> tableConfigDescriptor = new MapStateDescriptor<>(
                "table-process-state",
                String.class,
                TableProcess.class
        );
        BroadcastStream<String> broadcast = mysqlSource.broadcast(tableConfigDescriptor);

        //TODO 7、连接流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcast);

        //TODO 6、处理维度表数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectDS.process(new MyBroadcastFunction(tableConfigDescriptor));

        dimDS.print("HBase>>>>>>");*/


//        env.execute("DimSinkApp");

    }


}
