package org.example;


/*
 *
 * @author liukai
 * @date ${DATE} ${TIME}
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonJoinDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // 🔥 必须开 checkpoint！
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE CATALOG paimon_catalog WITH ("
                        + "'type' = 'paimon',"
                        + "'warehouse' = './paimon_warehouse')");
        tableEnv.executeSql("USE CATALOG paimon_catalog");


        // 3. 维表：用户表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS paimon_user ("
                        + "    user_id INT PRIMARY KEY NOT ENFORCED,"
                        + "    user_name STRING,"
                        + "    city STRING) WITH (\n" +
                "    'write-mode' = 'change-log',  \n" +
                "    'commit.interval' = '3 s',   \n" +
                "    'snapshot.time-retained' = '1 h' \n" +
                ")");



        // ==============================================
        // 【修复】维表 datagen_user
        // ==============================================
        tableEnv.executeSql("CREATE TEMPORARY TABLE datagen_user ("
                + "    user_id INT,"
                + "    user_name STRING,"
                + "    city STRING"
                + ") WITH ("
                + "    'connector' = 'datagen',"
                + "    'fields.user_id.kind' = 'random',"  // 这里改成 random
                + "    'fields.user_id.min' = '1',"
                + "    'fields.user_id.max' = '10'"
                + ")");

        // 写入维表
        tableEnv.executeSql("INSERT INTO paimon_user SELECT * FROM datagen_user").await();


    }
}