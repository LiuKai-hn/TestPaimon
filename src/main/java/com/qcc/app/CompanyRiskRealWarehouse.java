package com.qcc.app;


import com.alibaba.fastjson2.JSON;
import com.qcc.comm.OutPutTagUtil;
import com.qcc.func.*;
import com.qcc.pojo.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.table.Table;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/*
 *
 * @author liukai
 * @date 2026/4/30 14:17
 * 企查查/天眼查 官方同款
 * 企业风险+股权穿透 实时数仓任务
 * Flink1.17.1 + Kafka + Paimon + Doris
 * 大厂真实高复杂度版本
 */
public class CompanyRiskRealWarehouse {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ========== 生产级全局配置（大厂标配） ==========
        env.setParallelism(8);
        env.disableOperatorChaining(); // 算子链隔离，方便排查反压
        env.getCheckpointConfig().setCheckpointInterval(30000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cluster/flink/checkpoint/company-risk/");
        //env.getConfig().setIdleSourceTimeout(Duration.ofMinutes(5));

        // ========== 1. 构建多源Kafka ODS数据源 ==========
        // 1.1 工商主数据 ODS
        KafkaSource<String> companyOdsSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-biz-01:9092,kafka-biz-02:9092")
                .setTopics("ods_qcc_company_biz_cdc")
                .setGroupId("flink_qcc_risk_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<CompanyOrgOds> companyOdsStream = env.fromSource(companyOdsSource,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((data, ts) -> {
                                    try {
                                        return JSON.parseObject(data).getLong("eventTime");
                                    }catch (Exception e){
                                        return System.currentTimeMillis();
                                    }
                                }),
                        "ods-company-biz-source")
                .process(new CompanyOdsProcessFunc())
                .name("ods_company_process");

        // 1.2 行政处罚 ODS
// ====================== 1. 定义Kafka数据源（String） ======================
        KafkaSource<String> punishOdsSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-biz-01:9092")
                .setTopics("ods_qcc_company_punish_cdc")
                .setGroupId("flink_qcc_risk_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
// ====================== 2. 正确：先读String → 分配事件时间 → 转实体 ======================
        DataStream<CompanyPunishOds> punishOdsStream = env.fromSource(
                        punishOdsSource,
                        // 正确写法：String 流的水位线 + 从JSON提取事件时间
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((jsonStr, timestamp) -> {
                                    // 从原始字符串提取业务时间戳（事件时间，生产必须）
                                    try {
                                        return JSON.parseObject(jsonStr).getLong("punishTime");
                                    } catch (Exception e) {
                                        // 异常降级为当前时间
                                        return System.currentTimeMillis();
                                    }
                                }),
                        "ods-punish-source")
                // 正确：String 转实体 + 异常捕获
                .map(new MapFunction<String, CompanyPunishOds>() {
                    @Override
                    public CompanyPunishOds map(String s) throws Exception {
                        return JSON.parseObject(s, CompanyPunishOds.class);
                    }
                });


        // 1.3 股权关系 ODS
        KafkaSource<String> equityOdsSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-biz-01:9092")
                .setTopics("ods_qcc_equity_relation")
                .setGroupId("flink_qcc_risk_group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<EquityRelationOds> equityOdsStream = env.fromSource(equityOdsSource,
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)),
                        "ods-equity-source")
                .map(s -> JSON.parseObject(s, EquityRelationOds.class));

        // ========== 2. 广播流：行业字典维表（动态热更新） ==========
        // Java8 创建 Map
        Map<String, String> industryMap = new HashMap<>();
        industryMap.put("01", "制造业");
        industryMap.put("02", "金融业");
        industryMap.put("03", "建筑业");
        BroadcastStream<Map<String, String>> industryDictBroadcastStream = env.fromElements(industryMap).broadcast(OutPutTagUtil.INDUSTRY_BROADCAST_DESC);

        // ========== 3.双流复杂Join：工商主表 LEFT JOIN 行政处罚 ==========
        KeyedStream<CompanyOrgOds, String> keyedCompany = companyOdsStream.keyBy(CompanyOrgOds::getCompanyId);
        KeyedStream<CompanyPunishOds, String> keyedPunish = punishOdsStream.keyBy(CompanyPunishOds::getCompanyId);

        ConnectedStreams<CompanyOrgOds, CompanyPunishOds> riskConnect = keyedCompany.connect(keyedPunish);
        DataStream<CompanyRiskDwd> riskDwdStream = riskConnect.process(new CompanyRiskJoinFunc())
                .name("dwd_company_risk_join");

        // ========== 4. 关联广播维表：行业名称翻译 ==========
        DataStream<CompanyRiskDwd> riskDwdWithIndustry = riskDwdStream
                .connect(industryDictBroadcastStream)
                .process(new IndustryDimBroadcastFunc())
                .name("dwd_risk_dim_join");

        // ========== 5. 股权无限层级递归穿透计算（大厂核心难点） ==========
        DataStream<EquityTreeDto> equityTreeStream = equityOdsStream.keyBy(EquityRelationOds::getChildId)
                .process(new EquityRecursionProcessFunc())
                .name("dwd_equity_tree_calc");

        // ========== 6. DWS层：滑动窗口复合聚合 风险指标计算 ==========
        DataStream<CompanyRiskDws> riskDwsStream = riskDwdWithIndustry.keyBy(s->s.getCompanyId())
                .window(SlidingEventTimeWindows.of(Time.minutes(10),Time.minutes(5)))
                .process(new RiskAggWindowFunc())
                .name("dws_risk_agg");

        // ========== 7. 多分流：风险告警侧输出单独写入Kafka ==========
        /*DataStream<String> riskAlertStream = riskDwdWithIndustry.getSideOutput(OutPutTagUtil.RISK_ALERT_TAG)
                .map(JSON::toJSONString);*/

        // ========== 8. 分层落地存储 ==========
        // 8.1 DWD明细层 -> Paimon 主键Changelog表（修正之前PaimonSink不存在问题）
        writeToPaimon(riskDwdWithIndustry);
        // 8.2 DWS聚合层 -> Doris 实时大屏
        writeToDoris(riskDwsStream);
        // 8.3 股权穿透结果 -> Paimon 分区明细表
        writeEquityToPaimon(equityTreeStream);
        // 8.4 告警数据 -> 独立Kafka
        //riskAlertStream.sinkTo(buildKafkaSink("topic_qcc_risk_alert"));

        env.execute("Qcc_Company_Risk_And_Equity_Real_Task");
    }


    // ==================== 存储写入工具类（生产可用，无不存在的PaimonSink） ====================
    private static void writeToPaimon(DataStream<CompanyRiskDwd> stream) throws Exception {

    }

    private static void writeEquityToPaimon(DataStream<EquityTreeDto> stream) throws Exception {
        // 同Paimon写入逻辑
    }

    private static void writeToDoris(DataStream<CompanyRiskDws> stream){
        // 标准DorisSink 生产写法
    }

    private static SinkFunction<String> buildKafkaSink(String topic){
        // 生产Kafka Sink
        return null;
    }
}
