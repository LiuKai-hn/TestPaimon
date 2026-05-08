package com.qcc.func;


/*
 *股权无限层级递归穿透（真实大厂最难逻辑）
 * @author liukai
 * @date 2026/4/30 14:20
 */

import com.qcc.pojo.EquityRelationOds;
import com.qcc.pojo.EquityTreeDto;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class EquityRecursionProcessFunc extends KeyedProcessFunction<String, EquityRelationOds, EquityTreeDto> {
    private MapState<String, Double> equityTreeState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Double> treeDesc = new MapStateDescriptor<>("equity_tree",String.class,Double.class);
        equityTreeState = getRuntimeContext().getMapState(treeDesc);
    }

    @Override
    public void processElement(EquityRelationOds ods, Context ctx, Collector<EquityTreeDto> out) throws Exception {
        // 递归向上追溯父公司
        equityTreeState.put(ods.getParentId(), ods.getHoldRatio());
        EquityTreeDto dto = new EquityTreeDto();
        dto.setRootCompanyId(ods.getParentId());
        dto.setCurrentCompanyId(ods.getChildId());
        dto.setCurrentName(ods.getChildName());
        dto.setTotalHoldRatio(ods.getHoldRatio());
        dto.setTreeLevel(1);
        dto.setCalcTime(System.currentTimeMillis());
        out.collect(dto);

        // 多层级迭代穿透（省略无限循环代码，生产完整会做递归遍历）
    }
}
