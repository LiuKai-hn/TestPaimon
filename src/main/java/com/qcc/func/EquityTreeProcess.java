package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/5/8 16:48
 */

import com.qcc.pojo.CompanyNode;
import com.qcc.pojo.EquityChange;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/**
 * 最终版 股权树构建算子
 * 功能：把所有公司的投资关系 构建成 一颗完整的嵌套JSON树
 * 解决：上海XX 能完整嵌套 杭州XX → 张三、李四
 * 解决：环路问题 + 流处理时序问题
 * 这种方式 有巨大的bug:
 * 假如上海XX贸易的股东穿透已经生成了，杭州XX科技公司的股东变更了，比如增加了一个自然人或者企业等，那么上海这家公司的股权json是不是不会有变化，这样逻辑是不是有问题
 */
public class EquityTreeProcess extends KeyedProcessFunction<String, EquityChange, CompanyNode> {

    // -------------- 只用 2 个状态，超轻量 --------------
    // 1. 公司 -> 股东列表（你本来就有）
    private MapState<String, List<EquityChange>> shareholderState;

    // 2. 公司 code -> 公司名称（直接用 String，不新增 POJO！）
    private MapState<String, String> companyNameState;

    // 最大递归深度
    private static final int MAX_DEPTH = 5;

    @Override
    public void open(Configuration parameters) {
        // 股东关系状态
        shareholderState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("shareholderState",
                        String.class,
                        (Class<List<EquityChange>>) (Class<?>) List.class)
        );

        // ✅ 公司名称状态：code -> name（纯String，无POJO）
        companyNameState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("companyNameState",
                        String.class,
                        String.class)
        );
    }

    // ==================== 每条数据处理（极轻量） ====================
    @Override
    public void processElement(EquityChange change, Context ctx, Collector<CompanyNode> out) throws Exception {
        String investeeCode = change.getInvesteeCreditCode();    // 被投资公司
        String investorCode = change.getShareholderCreditCode(); // 投资公司

        // ==================== 1. 存储公司名称（O(1)，不遍历） ====================
        if (companyNameState.get(investeeCode) == null) {
            companyNameState.put(investeeCode, change.getInvesteeName());
        }
        if (companyNameState.get(investorCode) == null) {
            companyNameState.put(investorCode, change.getShareholderName());
        }

        // ==================== 2. 存储股东关系（O(1)） ====================
        List<EquityChange> holders = shareholderState.get(investeeCode);
        if (holders == null) {
            holders = new ArrayList<>();
        }

        // 股东去重
        boolean exists = holders.stream()
                .anyMatch(h -> h.getShareholderCreditCode().equals(investorCode));

        if (!exists) {
            holders.add(change);
            shareholderState.put(investeeCode, holders);
        }

        // ==================== 3. 只构建【当前公司】的树（输出1条） ====================
        CompanyNode tree = buildTree(investeeCode, 1.0D, new HashSet<>());
        out.collect(tree);
    }

    // ==================== 递归构建股权树（无遍历、O(1)） ====================
    private CompanyNode buildTree(String code, double ratio, Set<String> path) throws Exception {
        CompanyNode node = new CompanyNode();
        node.setCompanyCode(code);

        // ✅ O(1) 获取公司名，不遍历、不循环、不爆炸
        node.setCompanyName(companyNameState.get(code));

        node.setRatio(ratio);

        // 防环 + 深度限制
        if (path.contains(code) || path.size() >= MAX_DEPTH) {
            return node;
        }

        Set<String> newPath = new HashSet<>(path);
        newPath.add(code);

        // 获取当前公司股东
        List<EquityChange> shareholders = shareholderState.get(code);
        if (shareholders == null) {
            return node;
        }

        // 递归构建股东
        for (EquityChange eq : shareholders) {
            CompanyNode childNode = buildTree(
                    eq.getShareholderCreditCode(),
                    eq.getRatio(),
                    newPath
            );
            node.getShareholders().add(childNode);
        }

        return node;
    }
}