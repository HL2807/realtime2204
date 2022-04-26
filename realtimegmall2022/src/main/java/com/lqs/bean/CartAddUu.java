package com.lqs.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Author lqs
 * @Date 2022年04月24日 14:33:47
 * @Version 1.0.0
 * @ClassName CartAddUu
 * @Describe 用户加购求和事实表
 */

@Data
@AllArgsConstructor
public class CartAddUu {

    /** 窗口起始时间 */
    String stt;
    /** 窗口闭合时间 */
    String edt;
    /** 加购独立用户数 */
    Long cartAddUuCt;
    /** 时间戳 */
    Long ts;

}
