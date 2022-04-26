package com.lqs.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Author lqs
 * @Date 2022年04月22日 20:36:10
 * @Version 1.0.0
 * @ClassName TransientSink
 * @Describe 设置哪些字段不写入clickHouse
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {
}
