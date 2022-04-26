package com.lqs.app.functions;

import com.alibaba.fastjson.JSONObject;

/**
 * @Author lqs
 * @Date 2022年04月26日 19:02:34
 * @Version 1.0.0
 * @ClassName DimJoinFunction
 * @Describe 需求使用者直接实现的方法
 */
public interface DimJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo);

}
