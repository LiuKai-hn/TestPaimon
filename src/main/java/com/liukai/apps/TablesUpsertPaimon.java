package com.liukai.apps;


/*
 *
 * @author liukai
 * @date 2026/4/21 17:35
 */

import com.common.CommBaseConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TablesUpsertPaimon extends CommBaseConfig {

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
                + " 'commit.interval' = '10s'"
                + ")");

        // =========================================
        // 2. 三张维表源（生产 = MySQL CDC）
        // =========================================
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS default_catalog.default_database.user_info ("
                + " user_id     INT,"
                + " name        STRING,"
                + " update_time TIMESTAMP"
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + " 'rows-per-second' = '1',"
                + " 'fields.user_id.kind' = 'sequence',"
                + " 'fields.user_id.start' = '1',"
                + " 'fields.user_id.end'   = '3',"
                + " 'fields.name.kind' = 'random',"
                + " 'fields.name.length' = '2'"
                + ")");

        tEnv.executeSql("CREATE TABLE IF NOT EXISTS default_catalog.default_database.user_level ("
                + " user_id     INT,"
                + " level       STRING,"
                + " update_time TIMESTAMP"
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + " 'rows-per-second' = '1',"
                + " 'fields.user_id.kind' = 'sequence',"
                + " 'fields.user_id.start' = '1',"
                + " 'fields.user_id.end'   = '3'"
                + ")");

        tEnv.executeSql("CREATE TABLE IF NOT EXISTS default_catalog.default_database.user_addr ("
                + " user_id     INT,"
                + " city        STRING,"
                + " update_time TIMESTAMP"
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + " 'rows-per-second' = '1',"
                + " 'fields.user_id.kind' = 'sequence',"
                + " 'fields.user_id.start' = '1',"
                + " 'fields.user_id.end'   = '3'"
                + ")");

        // =========================================
        // ✅【真正生产】所有维表直接插入同一张宽表
        // Flink 无状态、无计算、无JOIN
        // =========================================

        // 插入用户信息
/*
        tEnv.executeSql("INSERT INTO user_dim (user_id, name, update_time) "
                + "SELECT user_id, name, update_time FROM default_catalog.default_database.user_info");
*/



        // 插入用户等级
        tEnv.executeSql("INSERT INTO user_dim (user_id, level, update_time) "
                + "SELECT user_id, level, update_time FROM default_catalog.default_database.user_level");

        // 插入用户地址
   /*     tEnv.executeSql("INSERT INTO user_dim (user_id, city, update_time) "
                + "SELECT user_id, city, update_time FROM default_catalog.default_database.user_addr");
    */
    }
}
