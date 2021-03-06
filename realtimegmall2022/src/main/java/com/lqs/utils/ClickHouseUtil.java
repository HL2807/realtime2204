package com.lqs.utils;

import com.lqs.bean.TransientSink;
import com.lqs.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.planner.validate.ValidationResult;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author lqs
 * @Date 2022年04月22日 20:24:29
 * @Version 1.0.0
 * @ClassName ClickHouseUtil
 * @Describe clickHouse工具类
 */
public class ClickHouseUtil {

    public static <T>SinkFunction<T> getClickHouseSink(String sql){

        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //TODO 使用反射的方式提取字段
                        Class<?> clz = t.getClass();

                        //Method[] methods = clz.getMethods();
                        //for (Method method : methods) {
                        //    method.invoke(t);
                        //}

                        Field[] fields = clz.getDeclaredFields();

                        int offset=0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            field.setAccessible(true);

                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink!=null){
                                offset++;
                                continue;
                            }

                            //TODO 获取数据并给占位符赋值
                            Object value = field.get(t);
                            preparedStatement.setObject(i+1-offset,value);
                        }

                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );

    }

}
