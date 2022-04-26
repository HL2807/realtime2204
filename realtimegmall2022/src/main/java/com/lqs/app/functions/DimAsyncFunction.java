package com.lqs.app.functions;

import com.alibaba.fastjson.JSONObject;
import com.lqs.common.GmallConfig;
import com.lqs.utils.DimUtil;
import com.lqs.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author lqs
 * @Date 2022年04月26日 18:59:26
 * @Version 1.0.0
 * @ClassName DimAsyncFunction
 * @Describe 异步 IO 函数
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    /**方式1*/
    public DimAsyncFunction(String tableName){
        this.tableName=tableName;
    }
    /**方式而*/
    /*private String id;
    public DimAsyncFunction(String tableName,String id){
        this.tableName=tableName;
        this.id=id;
    }*/

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // 1、查询维表数据
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, getKey(input));

                //2、将维表数据补充都JavaBean中
                join(input,dimInfo);

                //3、将补充之后的数据输出
                resultFuture.complete(Collections.singletonList(input));
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        //再次查询补充信息
        System.out.println("timeOut:"+input);
    }

}
