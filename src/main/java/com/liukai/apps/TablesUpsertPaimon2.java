package com.liukai.apps;


/*
 *
 * @author liukai
 * @date 2026/4/21 17:35
 */

import com.common.CommBaseConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TablesUpsertPaimon2 extends CommBaseConfig {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = getEnv(5000);
        StreamTableEnvironment tEnv = getTableEnv(env);

        tEnv.executeSql("CREATE CATALOG paimon_catalog WITH ("
                + "'type' = 'paimon',"
                + "'warehouse' = './paimon_warehouse')");

        tEnv.executeSql("USE CATALOG paimon_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test");
        tEnv.executeSql("USE test");

        // =========================================
        // ✅【关键修复】Paimon 宽表（正确参数，无报错）
        // =========================================
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS user_dim ("
                + " user_id     INT,"
                + " name        STRING,"
                + " level       STRING,"
                + " city        STRING,"
                + " update_time TIMESTAMP,"
                + " PRIMARY KEY (user_id) NOT ENFORCED"    // 自动取最新
                + ") WITH ("
                + " 'changelog-mode' = 'full',"
                + " 'write-mode' = 'change-log',"
                + " 'merge-engine' = 'partial-update',"
                + " 'partial-update.ignore-delete' = 'true',"
                + " 'changelog-producer' = 'full-compaction',"
                + " 'commit.interval' = '10s'"
                + ")");


        /*tEnv.executeSql(""
                + "INSERT INTO user_dim (user_id, name) VALUES "
                + "(1, '张三'),"
                + "(2, '李四'),"
                + "(3, '王五')");*/

        tEnv.executeSql(""
                + "INSERT INTO user_dim (user_id, level) VALUES "
                + "(1, 'V5'),"
                + "(2, 'V6'),"
                + "(3, 'V7')");

        /*tEnv.executeSql(""
                + "INSERT INTO user_dim (user_id, city) VALUES "
                + "(1, '北京'),"
                + "(2, '上海'),"
                + "(3, '广州')");*/


    }
}
