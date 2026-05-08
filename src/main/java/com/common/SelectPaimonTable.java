package com.common;


/*
 *
 * @author liukai
 * @date ${DATE} ${TIME}
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SelectPaimonTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE CATALOG paimon_catalog WITH ("
                        + "'type' = 'paimon',"
                        + "'warehouse' = './paimon_warehouse')");
        tableEnv.executeSql("USE CATALOG paimon_catalog");

        tableEnv.executeSql("SELECT * FROM test.user_dim /* OPTIONS('streaming'='true') */").print();

        //env.execute("Paimon-Datagen-Join-Final");
    }
}