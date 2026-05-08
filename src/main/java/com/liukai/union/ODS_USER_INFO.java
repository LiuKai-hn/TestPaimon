package com.liukai.union;


/*
 *
 * @author liukai
 * @date 2026/4/21 10:35
 */

import com.common.CommBaseConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ODS_USER_INFO extends CommBaseConfig {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = getEnv(5000);
        StreamTableEnvironment tableEnv = getTableEnv(env);

        tableEnv.executeSql("CREATE CATALOG paimon_catalog WITH ("
                + "'type' = 'paimon',"
                + "'warehouse' = './paimon_warehouse')");

        // 选择 flink 默认 catalog
        tableEnv.executeSql("USE CATALOG default_catalog");


        // ==============================================
        // 3. 切回默认 catalog，创建 datagen 源表（关键！）
        // ==============================================
        tableEnv.executeSql("USE CATALOG default_catalog");

        // ==============================================
        // ✅ ✅ ✅ 关键：固定数据！只产生 3 条固定数据！
        // ==============================================
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS default_catalog.default_database.user_info ("
                + " user_id INT,"
                + " name STRING"
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + " 'rows-per-second' = '2',"           // 速度慢
                + " 'fields.user_id.kind' = 'random',"   // 随机int
                + " 'fields.user_id.min' = '1',"         // 最小 1
                + " 'fields.user_id.max' = '3',"         // 最大 3
                + " 'fields.name.kind' = 'random',"
                + " 'fields.name.length' = '2'"
                + ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS default_catalog.default_database.user_level ("
                + " user_id INT,"
                + " level STRING"
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + " 'rows-per-second' = '2',"
                + " 'fields.user_id.kind' = 'random',"
                + " 'fields.user_id.min' = '1',"
                + " 'fields.user_id.max' = '3'"
                + ")");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS default_catalog.default_database.user_addr ("
                + " user_id INT,"
                + " city STRING"
                + ") WITH ("
                + " 'connector' = 'datagen',"
                + " 'rows-per-second' = '2',"
                + " 'fields.user_id.kind' = 'random',"
                + " 'fields.user_id.min' = '1',"
                + " 'fields.user_id.max' = '3'"
                + ")");


        tableEnv.executeSql("USE CATALOG paimon_catalog");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS default_db");
        tableEnv.executeSql("USE default_db");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS user_dim ("
                + "    user_id     INT,"
                + "    name        STRING,"
                + "    level       STRING,"
                + "    city        STRING,"
                + "    PRIMARY KEY (user_id) NOT ENFORCED"
                + ") WITH ("
                + "    'changelog-mode' = 'full',"
                + "    'write-mode' = 'change-log',"
                + "    'commit.interval' = '5s',"
                + "    'snapshot.num-retained.max' = '5',"
                + "    'merge-engine' = 'deduplicate'"
                + ")");

        // ==============================================
        // 4. 核心：多流合并 + 写入 Paimon 维表（正确写法）
        // ==============================================
        tableEnv.executeSql(""
                + "INSERT INTO paimon_catalog.default_db.user_dim "
                + "SELECT "
                + "    user_id, "
                + "    MAX(name)  AS name, "
                + "    MAX(level) AS level, "
                + "    MAX(city)  AS city "
                + "FROM ( "
                + "    SELECT user_id, name, CAST(NULL AS STRING) AS level, CAST(NULL AS STRING) AS city FROM default_catalog.default_database.user_info "
                + "    UNION ALL "
                + "    SELECT user_id, CAST(NULL AS STRING) AS name, level, CAST(NULL AS STRING) AS city FROM default_catalog.default_database.user_level "
                + "    UNION ALL "
                + "    SELECT user_id, CAST(NULL AS STRING) AS name, CAST(NULL AS STRING) AS level, city FROM default_catalog.default_database.user_addr "
                + ") t "
                + "GROUP BY user_id "
                + "");

    }

}
