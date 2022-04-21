package com.lqs.bean;

import lombok.Data;

/**
 * @Author lqs
 * @Date 2022年04月20日 21:59:32
 * @Version 1.0.0
 * @ClassName OrderInfoRefundBean
 * @Describe 退单事物表实体类
 */

@Data
public class OrderInfoRefund {
    // 订单 id
    String id;
    // 省份 id
    String province_id;
    // 历史数据
    String old;
}
