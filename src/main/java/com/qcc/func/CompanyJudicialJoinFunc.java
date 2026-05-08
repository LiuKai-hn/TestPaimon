package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:58
 */

import com.qcc.pojo.CompanyWide;
import com.qcc.pojo.JudicialRisk;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class CompanyJudicialJoinFunc extends KeyedCoProcessFunction<String, CompanyWide, JudicialRisk, CompanyWide> {

    @Override
    public void processElement1(CompanyWide wide, Context ctx, Collector<CompanyWide> out) {
        out.collect(wide);
    }

    @Override
    public void processElement2(JudicialRisk risk, Context ctx, Collector<CompanyWide> out) {
        CompanyWide wide = new CompanyWide();
        wide.setCompanyId(risk.getCompanyId());
        wide.setCaseType(risk.getCaseType());
        wide.setCaseCount(risk.getCaseCount());
        wide.setEventTime(risk.getPublishTs());
        wide.setOpType(risk.getOpType());
        out.collect(wide);
    }
}

