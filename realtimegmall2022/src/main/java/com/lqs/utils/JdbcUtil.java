package com.lqs.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.lqs.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年04月26日 18:28:30
 * @Version 1.0.0
 * @ClassName JdbcUtil
 * @Describe Jdbc查询通用工具类
 */
public class JdbcUtil {

    /**
     * 针对所有的JDBC服务的所有查询
     */
    public static <T> List<T> queryList(Connection connection,String querySql,Class<T> clz,boolean underScoreToCamel) throws Exception {

        //创建集合用于存放结果数据
        ArrayList<T> list = new ArrayList<>();

        //预编译Sql
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //执行查询操作
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        //遍历查询到的结果表，将每行数据封装为T对象，并将其放入集合
        while (resultSet.next()) {//行遍历
            //创建T对象
            T t = clz.newInstance();

            for (int i = 0; i < columnCount; i++) {//列遍历
                //获取列名
                String columnName = metaData.getColumnName(i + 1);
                //获取列值
                Object value = resultSet.getObject(columnName);

                //判断是否需要转换列名信息
                if(underScoreToCamel){
                    columnName= CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                //给T对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }

            //将T对象加入集合
            list.add(t);
        }

        //释放资源
        resultSet.close();
        preparedStatement.close();

        return list;

    }

    //测试
    public static void main(String[] args) throws Exception {

        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        System.out.println(queryList(connection,
                "select count(*) ct from GMALL211027_REALTIME.DIM_BASE_TRADEMARK",
                JSONObject.class,
                true));

        connection.close();
    }

}
