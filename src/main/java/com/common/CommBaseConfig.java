package com.common;


/*
 *
 * @author liukai
 * @date 2026/4/21 10:12
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CommBaseConfig {

    public static int checkInterval=5000;
    public static StreamExecutionEnvironment env;
    public static StreamExecutionEnvironment getEnv(int checkInterval){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(checkInterval); // 🔥 必须开 checkpoint！

        return env;
    }
    public static StreamTableEnvironment getTableEnv(StreamExecutionEnvironment env){
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        return tableEnv;
    }
}
