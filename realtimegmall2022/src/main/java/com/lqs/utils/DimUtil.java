package com.lqs.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lqs.common.GmallConfig;
import jdk.nashorn.internal.scripts.JD;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年04月26日 18:18:22
 * @Version 1.0.0
 * @ClassName DimUtil
 * @Describe Redis缓存jedis工具类
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection,String tableName, String id) throws Exception {

        //1、查询Redis
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoString = jedis.get(redisKey);

        if(dimInfoString!=null){
            //重置数据的过期时间
            jedis.expire(redisKey,24*60*60);
            //归还连接
            jedis.close();
            //返回数据结构
            return JSON.parseObject(dimInfoString);
        }

        //拼接sql语句
        String querySql="select * from "+ GmallConfig.HBASE_SCHEMA+"."+tableName+" where id='"+id+"'";
        System.out.println("Sql查询语句为：" + querySql);

        //查询Phoenix
        List<JSONObject> list = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        //因为只有一个数据，所以直接获取第一个即可
        JSONObject dimInfo = list.get(0);

        //将查询的数据写入Redis
        jedis.set(redisKey,dimInfo.toJSONString());
        jedis.expire(redisKey,24*60*60);
        jedis.close();

        //返回结果
        return dimInfo;

    }

    /**
     * 删除维表数据,删除Redis里面过期过期的数据
     */
    public static void delDimInfo(String tableName,String id){
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    /**
     * 测试工具
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end = System.currentTimeMillis();
        JSONObject dimInfo2 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end2 = System.currentTimeMillis();
        JSONObject dimInfo3 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end3 = System.currentTimeMillis();
        JSONObject dimInfo4 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "18");
        long end4 = System.currentTimeMillis();

        System.out.println(end - start); //145  138  148  136   140
        System.out.println(end2 - end);  //6    8    15   15    10
        System.out.println(end3 - end2); //1    0    1    1
        System.out.println(end4 - end3);

        System.out.println(dimInfo);
        System.out.println(dimInfo2);

        connection.close();
    }

}
