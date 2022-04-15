package com.lqs.app.functions;

import com.alibaba.fastjson.JSONObject;
import com.lqs.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @Author lqs
 * @Date 2022年04月13日 11:05:23
 * @Version 1.0.0
 * @ClassName DimSinkFunction
 * @Describe 保存维度到HBase(Phoenix)
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection= DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value：{"sinkTable":"dim_xxx","database":"gmall","table":"base_trademark","type":"insert",
    // "ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":"12","tm_name":"atguigu"},"old":{}}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement=null;

        try {
            //拼接SQL  upsert into db.tn(id,tm_name) values ('12','atguigu')
            String upsertSql = getUpsertSql(value.getString("sinkTable"), value.getJSONObject("data"));
            System.out.println("Phoenix sql语句"+upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);
            //执行写入操作
            preparedStatement.execute();

            connection.commit();

        }catch (SQLException e){
            System.out.println("插入数据失败！");
        }finally {
            //关闭连接，释放支援
            if (preparedStatement!=null){
                preparedStatement.close();
            }
        }

    }

    /**
     *
     * @param sinkTable tn
     * @param data {"id":"12","tm_name":"lqs"}
     * @return upsert into db.tn(id,tm_name,logo_url) values ('12','lqs','/aaa/bbb')
     */
    private String getUpsertSql(String sinkTable, JSONObject data) {
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        //scala:list.mkString(",")   ["1","2","3"] => "1,2,3"
        //StringUtils.join(columns, ",")
        return "upsert into "+GmallConfig.HBASE_SCHEMA+"."+sinkTable+"(" +
                StringUtils.join(columns,",")+ ") values ('" +
                StringUtils.join(values,"','")+"')";
    }
}
