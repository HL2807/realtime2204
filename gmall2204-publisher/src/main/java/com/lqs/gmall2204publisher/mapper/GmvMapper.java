package com.lqs.gmall2204publisher.mapper;

import org.apache.ibatis.annotations.Select;

/**
 * @Author lqs
 * @Date 2022年04月28日 19:29:11
 * @Version 1.0.0
 * @ClassName GmvMapper
 * @Describe
 */
public interface GmvMapper {

    //查询ClickHouse,获取GMV总数
    @Select("select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date}")
    Double selectGmv(int date);

}
