package com.qcc.app;


import com.qcc.func.EquityTreeProcess;
import com.qcc.func.FinalEquityTreeProcess;
import com.qcc.pojo.CompanyNode;
import com.qcc.pojo.EquityChange;
import com.qcc.utils.JsonUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * 股权穿透 主程序 + 多场景测试用例
 * Java8 + Flink 1.17 直接运行
 */

public class EquityPenetrationMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.setParallelism(1); // 单并行方便查看结果
        // ======================
        // 1. 构造测试数据源（内置4大测试场景）
        // ======================
        DataStream<EquityChange> source = env.addSource(new SourceFunction<EquityChange>() {
            @Override
            public void run(SourceContext<EquityChange> ctx) {
                ctx.collect(new EquityChange("ZHANG", "张三", "HANGZHOU", "杭州XX科技", 0.6, System.currentTimeMillis()));
                ctx.collect(new EquityChange("LI", "李四", "HANGZHOU", "杭州XX科技", 0.4, System.currentTimeMillis()));
                ctx.collect(new EquityChange("HANGZHOU", "杭州XX科技", "SHANGHAI", "上海XX贸易", 0.7, System.currentTimeMillis()));

                try { TimeUnit.SECONDS.sleep(1); } catch (Exception e) {}
            }
            @Override
            public void cancel() {}
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<EquityChange>forMonotonousTimestamps()
                .withTimestampAssigner((e, t) -> e.getEventTime()));

        // 全局统一存储 → 一次性输出完整树
        DataStream<CompanyNode> result = source
                .keyBy(e -> "00") // 全部数据打入分区
                .process(new FinalEquityTreeProcess());

        result.map(JsonUtil::toJson).print("最终正确输出");
        // 执行
        env.execute("企查查-股权穿透实时计算");
    }
}
