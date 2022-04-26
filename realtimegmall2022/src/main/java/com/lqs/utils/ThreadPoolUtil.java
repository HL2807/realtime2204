package com.lqs.utils;

import lombok.SneakyThrows;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author lqs
 * @Date 2022年04月26日 19:06:56
 * @Version 1.0.0
 * @ClassName ThreadPoolUtil
 * @Describe 线程池获取工具类
 */
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil() {
    }

    //懒加载创建线程池
    public static ThreadPoolExecutor getThreadPoolExecutor(){
        if (threadPoolExecutor==null){
            synchronized (ThreadPoolUtil.class){
                if (threadPoolExecutor==null){
                    threadPoolExecutor=new ThreadPoolExecutor(
                            4,
                            20,
                            60,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>()
                    );
                }
            }
        }
        return threadPoolExecutor;
    }

    public static void main(String[] args) {

        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor();

        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    System.out.println("************" + Thread.currentThread().getName() + "************");
                    Thread.sleep(2000);
                }
            });
        }
    }

}
