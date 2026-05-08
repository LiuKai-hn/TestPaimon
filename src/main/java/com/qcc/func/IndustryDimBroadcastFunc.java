package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/4/30 14:18
 */

import com.qcc.comm.OutPutTagUtil;
import com.qcc.pojo.CompanyRiskDwd;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BaseBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;


/**
 * 广播维表关联
 */
public class IndustryDimBroadcastFunc extends BroadcastProcessFunction<CompanyRiskDwd, Map<String,String>, CompanyRiskDwd> {

    @Override
    public void processElement(CompanyRiskDwd companyRiskDwd, BroadcastProcessFunction<CompanyRiskDwd, Map<String, String>, CompanyRiskDwd>.ReadOnlyContext readOnlyContext, Collector<CompanyRiskDwd> collector) throws Exception {
        ReadOnlyBroadcastState<String, String> industryState = readOnlyContext.getBroadcastState(OutPutTagUtil.INDUSTRY_BROADCAST_DESC);
   /*     String industryName = industryState.get(CompanyRiskDwd.getIndustryCode());
        if (industryName == null) {
            industryName = "未知行业";
        }
        dwd.setIndustryName(industryName);*/
        // 高风险判定，打入告警侧输出
        if(companyRiskDwd.getPunishCount() > 3){
            readOnlyContext.output(OutPutTagUtil.RISK_ALERT_TAG, companyRiskDwd);
        }
        collector.collect(companyRiskDwd);
    }

    @Override
    public void processBroadcastElement(Map<String,String> dict, Context ctx, Collector<CompanyRiskDwd> out) throws Exception {
        BroadcastState<String, String> state = ctx.getBroadcastState(OutPutTagUtil.INDUSTRY_BROADCAST_DESC);
        for (Map.Entry<String, String> entry : dict.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            state.put(key, value);
        }
    }
}
