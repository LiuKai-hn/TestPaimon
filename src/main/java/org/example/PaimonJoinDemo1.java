package org.example;


/*
 *
 * @author liukai
 * @date ${DATE} ${TIME}
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonJoinDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // 🔥 必须开 checkpoint！
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE CATALOG paimon_catalog WITH ("
                        + "'type' = 'paimon',"
                        + "'warehouse' = './paimon_warehouse')");
        tableEnv.executeSql("USE CATALOG paimon_catalog");

        // 2. 主表：订单表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS paimon_order (\n" +
                "    order_id INT,\n" +
                "    user_id INT,\n" +
                "    order_amount DECIMAL(10,2),\n" +
                "    create_time TIMESTAMP,\n" +
                "    PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'write-mode' = 'change-log',  \n" +
                "    'commit.interval' = '3 s',   \n" +
                "    'snapshot.time-retained' = '1 h' \n" +
                ")");


        tableEnv.executeSql("CREATE TEMPORARY TABLE datagen_order ("
                + "    order_id INT,"
                + "    user_id INT,"
                + "    order_amount DECIMAL(10,2),"
                + "    create_time TIMESTAMP"
                + ") WITH ("
                + "    'connector' = 'datagen',"
                + "    'rows-per-second' = '1',"
                + "    'fields.order_id.kind' = 'random',"   // 这里改成 random
                + "    'fields.order_id.min' = '1',"
                + "    'fields.order_id.max' = '10000',"
                + "    'fields.user_id.min' = '1',"
                + "    'fields.user_id.max' = '10'"
                + ")");

        // 持续写入主表
        tableEnv.executeSql("INSERT INTO paimon_order SELECT * FROM datagen_order").await();


        tableEnv.executeSql("SELECT * FROM datagen_order").print();

        //env.execute("Paimon");
    }
}