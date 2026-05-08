package com.qcc.app;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:56
 */

import com.alibaba.fastjson2.JSON;
import com.qcc.func.CompanyEquityJoinFunc;
import com.qcc.func.CompanyJudicialJoinFunc;
import com.qcc.func.RiskWindowAggFunc;
import com.qcc.pojo.CompanyInfo;
import com.qcc.pojo.CompanyWide;
import com.qcc.pojo.EquityChange;
import com.qcc.pojo.JudicialRisk;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.paimon.options.Options;


public class QccRealTimeWarehouse {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生产配置
        env.setParallelism(6);
        env.enableCheckpointing(5 * 60 * 1000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoint/qcc");

        // ==================== 1. 消费 Kafka 3 条流 ====================
        // 工商信息流
        KafkaSource<String> companySource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-01:9092,kafka-02:9092")
                .setTopics("qcc_company_info")
                .setGroupId("qcc_group_01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<CompanyInfo> companyStream = env.fromSource(companySource,
                        WatermarkStrategy.<String>forMonotonousTimestamps()
                                .withTimestampAssigner((s, ts) -> JSON.parseObject(s).getLong("updateTs")),
                        "company_source")
                .map(s -> JSON.parseObject(s, CompanyInfo.class))
                .name("parse_company_stream");

        // 股权变更流
        KafkaSource<String> equitySource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-01:9092,kafka-02:9092")
                .setTopics("qcc_equity_change")
                .setGroupId("qcc_group_01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<EquityChange> equityStream = env.fromSource(equitySource,
                        WatermarkStrategy.<String>forMonotonousTimestamps()
                                .withTimestampAssigner((s, ts) -> JSON.parseObject(s).getLong("changeTs")),
                        "equity_source")
                .map(s -> JSON.parseObject(s, EquityChange.class))
                .name("parse_equity_stream");

        // 司法风险流
        KafkaSource<String> judicialSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-01:9092,kafka-02:9092")
                .setTopics("qcc_judicial_risk")
                .setGroupId("qcc_group_01")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<JudicialRisk> judicialStream = env.fromSource(judicialSource,
                        WatermarkStrategy.<String>forMonotonousTimestamps()
                                .withTimestampAssigner((s, ts) -> JSON.parseObject(s).getLong("publishTs")),
                        "judicial_source")
                .map(s -> JSON.parseObject(s, JudicialRisk.class))
                .name("parse_judicial_stream");

        // ==================== 2. Union 合并增量日志流（同结构才能Union） ====================
        // 模拟2条工商增量日志Union（生产常见：多机房/多topic合并）
        DataStream<CompanyInfo> companyUnionStream = companyStream.union(companyStream);

        // ==================== 3. KeyBy 按企业ID分组 ====================
        KeyedStream<CompanyInfo, String> keyedCompany = companyUnionStream
                .keyBy((KeySelector<CompanyInfo, String>) CompanyInfo::getCompanyId);

        KeyedStream<EquityChange, String> keyedEquity = equityStream
                .keyBy((KeySelector<EquityChange, String>) EquityChange::getShareholderCreditCode);

        KeyedStream<JudicialRisk, String> keyedJudicial = judicialStream
                .keyBy((KeySelector<JudicialRisk, String>) JudicialRisk::getCompanyId);

        // ==================== 4. Connect + KeyedCoProcess 实现流Join（核心算子） ====================
        // 工商 + 股权 join
        ConnectedStreams<CompanyInfo, EquityChange> connect1 = keyedCompany.connect(keyedEquity);
        DataStream<CompanyWide> companyEquityStream = connect1.process(new CompanyEquityJoinFunc())
                .name("company_equity_join");

        // 结果再 join 司法风险流
        KeyedStream<CompanyWide, String> keyedWide = companyEquityStream
                .keyBy(CompanyWide::getCompanyId);
        ConnectedStreams<CompanyWide, JudicialRisk> connect2 = keyedWide.connect(keyedJudicial);

        DataStream<CompanyWide> finalWideStream = connect2.process(new CompanyJudicialJoinFunc())
                .name("company_judicial_join");

        // ==================== 5. 窗口聚合（近5分钟企业风险统计） ====================
        DataStream<CompanyWide> windowAggStream = finalWideStream.keyBy(CompanyWide::getCompanyId)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new RiskWindowAggFunc())
                .name("5min_risk_agg");

        // ==================== 6. 写入 Paimon 主键表（实时明细存储） ====================
        Options paimonOptions = new Options();
        paimonOptions.set("warehouse", "hdfs:///paimon/warehouse");
        paimonOptions.set("database", "qcc_db");
        paimonOptions.set("table", "company_wide");


        // 3. 构造 FlinkSink（替代 PaimonSink）


        // ==================== 7. 写入 Doris 做OLAP分析 ====================


        env.execute("Qcc_RealTime_DataWarehouse_V1.0");
    }
}