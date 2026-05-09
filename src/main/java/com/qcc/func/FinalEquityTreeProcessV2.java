package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/5/8 19:09
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
 * 股权穿透实时构建算子【最终正确可上线版】
 * 核心业务场景：
 *  自然人/企业股东发生变更后，所有被它投资的下游企业、再下游企业 全链自动刷新股权树
 *
 * 核心两套关系：
 *  1. 正向关系：公司 -> 自己的直接股东列表  【用于：向下递归构建股权穿透树】
 *  2. 下游关系：投资方 -> 自己投资了哪些下游公司  【用于：变更后向下递归刷新全链】
 *
 * 两套递归：
 *  1. markDownstreamRefresh：递归【向下游传播刷新标记】（A变→A的下游全变）
 *  2. buildTree：递归【向下构建股权穿透树】（从当前公司一层层挖到最终自然人）
 */
public class FinalEquityTreeProcessV2 extends KeyedProcessFunction<String, EquityChange, CompanyNode> {

    // 业务无限穿透：设置99层，真实企业股权结构不会超过这个深度
    private static final int MAX_DEPTH = 99;

    // 延迟触发时间：本地10ms保证定时器必触发；生产改为300~1000ms做微批聚合，减少重复计算
    private static final long DELAY_MS = 10;

    // ======================== Flink 状态定义 ========================
    /**
     * 正向股东状态
     * Key：企业统一信用代码
     * Value：该企业的所有直接股东列表
     * 用途：buildTree 递归向下构建股权树时使用
     */
    private MapState<String, List<EquityChange>> shareholderState;

    /**
     * 下游关联状态【最核心】
     * Key：投资方编码（我是谁）
     * Value：我直接投资的所有下游企业编码集合
     * 用途：股东变更后，递归向下找到所有被投资的下游企业，全部标记刷新
     */
    private MapState<String, Set<String>> downstreamState;

    /**
     * 企业名称映射
     * Key：企业/个人编码
     * Value：企业/个人名称
     * 用途：O(1)快速获取名称，不遍历、不循环
     */
    private MapState<String, String> companyNameState;

    /**
     * 待刷新企业队列
     * 只存放需要重新构建股权树的企业，绝不遍历全量企业
     */
    private MapState<String, Boolean> needRefreshState;

    // ======================== 初始化所有状态 ========================
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化正向股东状态
        shareholderState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("shareholderState",
                        String.class, (Class<List<EquityChange>>) (Class<?>) List.class)
        );

        // 初始化下游关联状态
        downstreamState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("downstreamState",
                        String.class, (Class<Set<String>>) (Class<?>) Set.class)
        );

        // 初始化企业名称状态
        companyNameState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("companyNameState", String.class, String.class)
        );

        // 初始化待刷新队列状态
        needRefreshState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("needRefreshState", String.class, Boolean.class)
        );
    }

    // ======================== 每条股权变更数据流入处理 ========================
    @Override
    public void processElement(EquityChange change, Context ctx, Collector<CompanyNode> out) throws Exception {
        // investeeCode：被投资方 = 子公司（被别人投资的公司）
        String investeeCode = change.getInvesteeCreditCode();
        // investorCode：投资方 = 股东（投资别人的公司/自然人）
        String investorCode = change.getShareholderCreditCode();

        // 1. 缓存企业/个人名称：不存在才存入，避免重复覆盖
        if (companyNameState.get(investeeCode) == null) {
            companyNameState.put(investeeCode, change.getInvesteeName());
        }
        if (companyNameState.get(investorCode) == null) {
            companyNameState.put(investorCode, change.getShareholderName());
        }

        // 2. 维护正向股东关系：给【被投资方】添加一个直接股东
        List<EquityChange> holderList = shareholderState.get(investeeCode);
        // 空则初始化集合
        if (holderList == null) {
            holderList = new ArrayList<>();
        }
        // 去重：避免同一个股东重复加入列表
        boolean alreadyExist = holderList.stream()
                .anyMatch(item -> item.getShareholderCreditCode().equals(investorCode));
        if (!alreadyExist) {
            holderList.add(change);
            // 回写到状态
            shareholderState.put(investeeCode, holderList);
        }

        // 3. 维护下游关联关系【核心关键】
        // 关系：investorCode(投资方) 投资了 investeeCode(被投资方)
        // 所以：投资方的下游列表，要加入被投资方
        Set<String> downstreamSet = downstreamState.get(investorCode);
        if (downstreamSet == null) {
            downstreamSet = new HashSet<>();
        }
        downstreamSet.add(investeeCode);
        // 回写到下游状态
        downstreamState.put(investorCode, downstreamSet);

        // 4. 递归向下标记刷新：当前投资方变更，所有下游企业全部要刷新股权树
        markDownstreamRefresh(investorCode);

        // 5. 注册定时器：延迟统一触发构建输出，微批合并，避免频繁计算
        long timerTs = System.currentTimeMillis() + DELAY_MS;
        ctx.timerService().registerProcessingTimeTimer(timerTs);
    }

    // ======================== 递归方法一：向下游递归标记所有需要刷新的企业 ========================
    /**
     * 逻辑方向：自上而下
     * 示例：张三 -> 杭州公司 -> 上海公司 -> 集团公司
     * 张三变更：
     *  1. 先标记张三自己需要刷新
     *  2. 找到张三的下游【杭州公司】，标记并递归
     *  3. 找到杭州的下游【上海公司】，标记并递归
     *  4. 找到上海的下游【集团公司】，标记并递归
     *  整条链路全部加入待刷新队列
     */
    private void markDownstreamRefresh(String currentCode) throws Exception {
        // 第一步：先把自己标记为需要刷新
        needRefreshState.put(currentCode, true);

        // 第二步：获取当前企业所有直接下游公司
        Set<String> downstreamList = downstreamState.get(currentCode);
        // 没有下游，递归终止（出口条件，防止无限递归）
        if (downstreamList == null || downstreamList.isEmpty()) {
            return;
        }

        // 第三步：遍历每个下游，继续递归向下标记
        for (String downCode : downstreamList) {
            markDownstreamRefresh(downCode);
        }
    }

    // ======================== 定时器触发：批量构建并输出股权树 ========================
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CompanyNode> out) throws Exception {
        // 1. 取出所有待刷新的企业编码
        Set<String> refreshCompanySet = new HashSet<>();
        for (String code : needRefreshState.keys()) {
            refreshCompanySet.add(code);
        }
        // 2. 清空待刷新队列，准备下一轮接收变更
        needRefreshState.clear();

        // 3. 只遍历需要刷新的企业，逐个构建完整股权树输出
        // 优势：绝不遍历全量企业，性能极高
        for (String code : refreshCompanySet) {
            // 递归构建当前企业的完整股权穿透树
            CompanyNode treeNode = buildTree(code, 1.0D, new HashSet<>());
            out.collect(treeNode);
        }
    }

    // ======================== 递归方法二：向下递归构建完整股权穿透树 ========================
    /**
     * @param companyCode 当前要构建树的企业编码
     * @param ratio 当前企业对上一级的持股比例
     * @param path 已访问过的企业路径，用于防环路（A→B→A 循环持股）
     *
     * 逻辑方向：自上而下
     * 从当前企业开始，递归找自己的每一个股东，一直挖到无股东/自然人为止
     * 同时做两层保护：环路检测 + 最大深度限制，防止栈溢出和死循环
     */
    private CompanyNode buildTree(String companyCode, double ratio, Set<String> path) throws Exception {
        // 初始化树节点
        CompanyNode node = new CompanyNode();
        node.setCompanyCode(companyCode);
        // O(1) 获取企业名称
        node.setCompanyName(companyNameState.get(companyCode));
        // 设置持股比例
        node.setRatio(ratio);

        // 递归终止条件1：当前节点已经在路径中，出现循环持股，直接截断
        // 递归终止条件2：超过最大穿透深度99层，直接截断
        if (path.contains(companyCode) || path.size() >= MAX_DEPTH) {
            return node;
        }

        // 新建路径集合：避免不同递归分支共享路径，互相干扰
        Set<String> newPath = new HashSet<>(path);
        // 记录当前节点到访问路径中
        newPath.add(companyCode);

        // 获取当前企业的所有直接股东
        List<EquityChange> shareholderList = shareholderState.get(companyCode);
        // 没有股东，递归终止
        if (shareholderList == null || shareholderList.isEmpty()) {
            return node;
        }

        // 遍历每一个直接股东，递归构建子节点树
        for (EquityChange item : shareholderList) {
            CompanyNode childNode = buildTree(
                    item.getShareholderCreditCode(),  // 子节点编码（股东编码）
                    item.getRatio(),                 // 子节点持股比例
                    newPath                          // 传递已访问路径
            );
            // 加入当前节点的股东子列表
            node.getShareholders().add(childNode);
        }

        return node;
    }
}