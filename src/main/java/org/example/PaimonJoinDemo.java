package org.example;


/*
 *
 * @author liukai
 * @date ${DATE} ${TIME}
 */

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PaimonJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // 🔥 必须开 checkpoint！
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE CATALOG paimon_catalog WITH ("
                        + "'type' = 'paimon',"
                        + "'warehouse' = './paimon_warehouse')");
        tableEnv.executeSql("USE CATALOG paimon_catalog");


        // 4. JOIN 结果表
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS paimon_order_join_user ("
                        + "    order_id INT,"
                        + "    user_id INT,"
                        + "    user_name STRING,"
                        + "    city STRING,"
                        + "    order_amount DECIMAL(10,2),"
                        + "    create_time TIMESTAMP,"
                        + "    PRIMARY KEY (order_id) NOT ENFORCED) WITH (\n" +
                "    'write-mode' = 'change-log',  \n" +
                "    'commit.interval' = '3 s',   \n" +
                "    'snapshot.time-retained' = '1 h' \n" +
                ")");



        // 5. JOIN 写入结果表
        tableEnv.executeSql("INSERT INTO paimon_order_join_user "
                + "SELECT o.order_id, o.user_id, u.user_name, u.city, o.order_amount, o.create_time "
                + "FROM paimon_order o "
                + "LEFT JOIN paimon_user u ON o.user_id = u.user_id").await();


    }
}