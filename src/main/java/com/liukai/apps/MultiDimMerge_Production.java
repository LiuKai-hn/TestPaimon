package com.liukai.apps;


/*
 *
 * @author liukai
 * @date 2026/4/23 14:41
 */

import com.liukai.func.UserMergeProcessFunction;
import com.liukai.pojo.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/***
 * 多张维表合并成一张最终解决方案
 */
public class MultiDimMerge_Production {

    public static void main(String[] args) throws Exception {
        // =========================================
        // 1. 生产级环境 & 全局配置
        // =========================================
        Configuration conf = new Configuration();
        // 失败重启策略
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(3); // 按生产资源调整

        // ========== 生产级 Checkpoint 配置 ==========
        env.enableCheckpointing(60000); // 1 分钟 CK
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        // ========== 状态后端：RocksDB 增量 ==========
        EmbeddedRocksDBStateBackend rocksDBBackend = new EmbeddedRocksDBStateBackend(true); // true=开启增量
        env.setStateBackend(rocksDBBackend);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // =========================================
        // 2. 建 Paimon 宽表（生产 DDL）
        // =========================================
        tEnv.executeSql("CREATE CATALOG IF NOT EXISTS paimon_catalog WITH (" +
                "  'type' = 'paimon'," +
                "  'warehouse' = './paimon_warehouse'" +
                ")");
        tEnv.executeSql("USE CATALOG paimon_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS dim");
        tEnv.executeSql("USE dim");

        tEnv.executeSql("" +
                "CREATE TABLE IF NOT EXISTS dim_user_full (" +
                "  user_id     INT PRIMARY KEY," +
                "  name        STRING," +
                "  level       STRING," +
                "  city        STRING," +
                "  update_time TIMESTAMP" +
                ") WITH (" +
                "  'changelog-producer' = 'full-compaction'," +
                "  'merge-engine' = 'deduplicate'," +
                "  'commit.interval' = '30 s'," +
                "  'snapshot.num-retained.max' = '10'," +
                "  'snapshot.num-retained.min' = '5'" +
                ")"
        );

        // =========================================
        // 3. 模拟三个维表流（生产替换为 MySQL CDC）
        // =========================================
        DataStream<UserInfo> infoStream = env.fromElements(
                new UserInfo(1, "张三"),
                new UserInfo(2, "李四"),
                new UserInfo(1, "张三丰")
        );

        DataStream<UserLevel> levelStream = env.fromElements(
                new UserLevel(1, "V1"),
                new UserLevel(2, "V2"),
                new UserLevel(1, "V9")
        );

        DataStream<UserAddr> addrStream = env.fromElements(
                new UserAddr(1, "北京"),
                new UserAddr(2, "上海"),
                new UserAddr(1, "深圳")
        );

        // =========================================
        // 4. 统一 Watermark + 防空闲阻塞（核心）
        // =========================================
        WatermarkStrategy<UniformMessage> wms = WatermarkStrategy
                .<UniformMessage>forMonotonousTimestamps()
                .withIdleness(Duration.ofSeconds(15)) // 15s 无数据则标记空闲
                .withTimestampAssigner((e, ts) -> System.currentTimeMillis());

        // 统一结构
        DataStream<UniformMessage> s1 = infoStream.map(r -> new UniformMessage(r.getUserId(), r.getName(), null, null)).assignTimestampsAndWatermarks(wms);
        DataStream<UniformMessage> s2 = levelStream.map(r -> new UniformMessage(r.getUserId(), null, r.getLevel(), null)).assignTimestampsAndWatermarks(wms);
        DataStream<UniformMessage> s3 = addrStream.map(r -> new UniformMessage(r.getUserId(), null, null, r.getCity())).assignTimestampsAndWatermarks(wms);

        // =========================================
        // 5. 多流合并 + KeyedState 合并字段（无NULL）
        // =========================================
        DataStream<UserFull> fullStream = s1.union(s2, s3)
                .keyBy((KeySelector<UniformMessage, Integer>) UniformMessage::getUserId)
                .process(new UserMergeProcessFunction());

        // =========================================
        // 6. 写入 Paimon
        // =========================================
        Table table = tEnv.fromDataStream(fullStream,
                $("user_id"),
                $("name"),
                $("level"),
                $("city"),
                $("update_time").as("update_time")
        );
        table.executeInsert("paimon.dim.dim_user_full");

        env.execute("MultiDim-Merge-To-Paimon-Prod");
    }


}