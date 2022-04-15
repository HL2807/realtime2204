package com.lqs.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @Author lqs
 * @Date 2022年04月11日 21:27:31
 * @Version 1.0.0
 * @ClassName KafkaUtil
 * @Describe Kafka工具类
 * 和 Kafka 交互要用到 Flink 提供的 FlinkKafkaConsumer、FlinkKafkaProducer 类，为了提高模板代码的复用性，
 * 将其封装到 KafkaUtil 工具类中。
 *
 */
public class KafkaUtil {

    static String BOOTSTRAP_SERVERS = "nwh120:9092";
    static String DEFAULT_TOPIC = "default_topic";
    static Properties properties = new Properties();

    /**
     *  此处从 Kafka 读取数据，创建 getKafkaConsumer(String topic, String groupId) 方法
     * @param topic 主题
     * @param groupId 组id
     * @return FlinkKafkaConsumer
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if (record == null || record.value() == null) {
                            return "";
                        } else {
                            return new String(record.value());
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties);

    }

    public static SinkFunction<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }
}
