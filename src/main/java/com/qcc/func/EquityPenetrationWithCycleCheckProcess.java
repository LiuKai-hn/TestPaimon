package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/5/6 17:01
 */

import com.qcc.pojo.EquityChange;
import com.qcc.pojo.EquityPath;
import com.qcc.utils.EntityUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** 企查查/天眼查 股权穿透核心算子 (最终生产版)
        * 支持：多股东 + 环路检测 + 自然人终止递归
        * 环境：Java8 + Flink 1.17
        */
public class EquityPenetrationWithCycleCheckProcess
        extends KeyedProcessFunction<String, EquityChange, EquityPath> {

    private static final int MAX_DEPTH = 5;

    private MapStateDescriptor<String, List<EquityChange>> companyShareholdersMapDesc;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        companyShareholdersMapDesc = new MapStateDescriptor<>(
                "company_shareholders_map",
                String.class,
                (Class<List<EquityChange>>) (Class<?>) List.class
        );

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(30))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        companyShareholdersMapDesc.enableTimeToLive(ttlConfig);
    }

    @Override
    public void processElement(
            EquityChange change,
            Context context,
            Collector<EquityPath> collector) throws Exception {

        MapState<String, List<EquityChange>> companyShareholdersMap =
                getRuntimeContext().getMapState(companyShareholdersMapDesc);

        String investeeCode = change.getInvesteeCreditCode();
        String investeeName = change.getInvesteeName();
        String targetCompanyKey = context.getCurrentKey();

        // 1. 更新股东列表
        List<EquityChange> shareholderList = companyShareholdersMap.get(investeeCode);
        if (shareholderList == null) {
            shareholderList = new ArrayList<>();
        }

        boolean exists = false;
        for (EquityChange e : shareholderList) {
            if (e.getShareholderCreditCode().equals(change.getShareholderCreditCode())) {
                exists = true;
                break;
            }
        }

        if (!exists) {
            shareholderList.add(change);
            companyShareholdersMap.put(investeeCode, shareholderList);
        }

        // ==============================================
        // 【修复核心】只穿透当前这条变更，不遍历全量股东
        // ==============================================
        List<String> initPath = new ArrayList<>();
        initPath.add(investeeName);

        Set<String> visitedCodes = new HashSet<>();
        visitedCodes.add(investeeCode);

        // 只处理当前这条股东，不再循环所有股东 → 彻底去重
        penetrateSingle(
                change,          // 传入当前变更
                investeeCode,
                investeeName,
                1,
                1.0,
                initPath,
                visitedCodes,
                companyShareholdersMap,
                collector,
                targetCompanyKey
        );
    }

    /**
     * 【修复版】只穿透【当前一条股东】，不会重复输出
     */
    private void penetrateSingle(
            EquityChange currentShareholder,
            String currentCompanyCode,
            String currentCompanyName,
            int depth,
            double totalRatio,
            List<String> path,
            Set<String> visitedCodes,
            MapState<String, List<EquityChange>> companyShareholdersMap,
            Collector<EquityPath> out,
            String targetCompanyKey
    ) throws Exception {

        if (depth > MAX_DEPTH) {
            return;
        }

        String shCode = currentShareholder.getShareholderCreditCode();
        String shName = currentShareholder.getShareholderName();
        double ratio = currentShareholder.getRatio();
        double newTotalRatio = totalRatio * ratio;

        List<String> newPath = new ArrayList<>(path);
        newPath.add(shName);

        // 环路检测
        if (visitedCodes.contains(shCode)) {
            return;
        }

        Set<String> newVisited = new HashSet<>(visitedCodes);
        newVisited.add(shCode);
        boolean isNaturalPerson = EntityUtil.isNaturalPerson(shCode);

        // 输出一条，唯一不重复
        out.collect(new EquityPath(
                shCode,
                shName,
                targetCompanyKey,
                currentCompanyName,
                newTotalRatio,
                depth,
                newPath,
                isNaturalPerson,
                System.currentTimeMillis()
        ));

        // 终止：自然人
        if (isNaturalPerson) {
            return;
        }

        // 继续向上查该股东的股东
        List<EquityChange> parentHolders = companyShareholdersMap.get(shCode);
        if (parentHolders != null && !parentHolders.isEmpty()) {
            for (EquityChange parent : parentHolders) {
                penetrateSingle(
                        parent,
                        shCode,
                        shName,
                        depth + 1,
                        newTotalRatio,
                        newPath,
                        newVisited,
                        companyShareholdersMap,
                        out,
                        targetCompanyKey
                );
            }
        }
    }
}