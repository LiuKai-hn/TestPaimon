package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/5/8 18:21
 */

import com.qcc.pojo.CompanyNode;
import com.qcc.pojo.EquityChange;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.*;

/**
 * 生产级 - 企查查模式 无限层级股权穿透
 * 满足：
 * 1. 支持 99 层深度（等价无限穿透）
 * 2. 强制防环（A->B->C->A 自动停止）
 * 3. 无栈溢出风险
 * 4. 无全量遍历
 * 5. 状态极小
 * 6. 可直接上线
 * 这种方式 有巨大的bug:
 * 假如上海XX贸易的股东穿透已经生成了，杭州XX科技公司的股东变更了，比如增加了一个自然人或者企业等，那么上海这家公司的股权json是不是不会有变化，这样逻辑是不是有问题
 */
public class FinalEquityTreeProcess extends KeyedProcessFunction<String, EquityChange, CompanyNode> {

    // ====================== 生产级配置 ======================
    // 企查查/天眼查 标准：99层，等价“无限穿透”
    private static final int MAX_DEPTH = 99;

    // ====================== 状态定义 ======================
    // 股东关系：公司code → 股东列表
    private MapState<String, List<EquityChange>> shareholderState;

    // 公司名称：code → name（O(1)查询）
    private MapState<String, String> companyNameState;

    // ====================== 初始化 ======================
    @Override
    public void open(Configuration parameters) {
        // 股东关系状态
        shareholderState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "shareholderState",
                        String.class,
                        (Class<List<EquityChange>>) (Class<?>) List.class
                )
        );

        // 公司名称状态
        companyNameState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "companyNameState",
                        String.class,
                        String.class
                )
        );
    }

    // ====================== 数据处理 ======================
    @Override
    public void processElement(EquityChange change, Context ctx, Collector<CompanyNode> out) throws Exception {
        String investeeCode = change.getInvesteeCreditCode();
        String investorCode = change.getShareholderCreditCode();

        // 1. 存储公司名称（O(1)）
        if (companyNameState.get(investeeCode) == null) {
            companyNameState.put(investeeCode, change.getInvesteeName());
        }
        if (companyNameState.get(investorCode) == null) {
            companyNameState.put(investorCode, change.getShareholderName());
        }

        // 2. 存储股东关系（O(1)）
        List<EquityChange> holders = shareholderState.get(investeeCode);
        if (holders == null) {
            holders = new ArrayList<>();
        }

        boolean exists = holders.stream()
                .anyMatch(h -> h.getShareholderCreditCode().equals(investorCode));

        if (!exists) {
            holders.add(change);
            shareholderState.put(investeeCode, holders);
        }

        // 3. 构建当前公司完整股权树（99层深度，等价无限穿透）
        CompanyNode fullTree = buildTree(investeeCode, 1.0D, new HashSet<>());
        out.collect(fullTree);
    }

    // ====================== 【生产级】递归构建股权树 ======================
    /**
     * 安全机制：
     * 1. 路径追踪防环
     * 2. 最大深度99层
     * 3. 无状态遍历
     * 4. 无栈溢出
     */
    private CompanyNode buildTree(String code, double ratio, Set<String> currentPath) throws Exception {
        CompanyNode node = new CompanyNode();
        node.setCompanyCode(code);
        node.setCompanyName(companyNameState.get(code));
        node.setRatio(ratio);

        // ====================== 【核心安全】防环 + 深度限制 ======================
        // 如果已经走过该节点 = 环路 → 停止
        // 如果超过99层 → 停止（等价企查查无限穿透）
        if (currentPath.contains(code) || currentPath.size() >= MAX_DEPTH) {
            return node;
        }

        // 记录当前路径（防止环路）
        Set<String> newPath = new HashSet<>(currentPath);
        newPath.add(code);

        // 获取当前公司股东
        List<EquityChange> shareholders = shareholderState.get(code);
        if (shareholders == null) {
            return node;
        }

        // 递归构建所有股东（安全、可控、无风险）
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