package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:57
 */

import com.qcc.pojo.CompanyInfo;
import com.qcc.pojo.CompanyWide;
import com.qcc.pojo.EquityChange;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * 工商 + 股权 Join
 */
public class CompanyEquityJoinFunc
        extends KeyedCoProcessFunction<String, CompanyInfo, EquityChange, CompanyWide> {

    @Override
    public void processElement1(CompanyInfo info,
                                Context ctx,
                                Collector<CompanyWide> out) {
        CompanyWide wide = new CompanyWide();
        wide.setCompanyId(info.getCompanyId());
        wide.setCompanyName(info.getCompanyName());
        wide.setLegalPerson(info.getLegalPerson());
        wide.setStatus(info.getStatus());
        wide.setRegCapital(info.getRegCapital());
        wide.setEventTime(info.getUpdateTs());
        wide.setOpType(info.getOpType());
        out.collect(wide);
    }

    @Override
    public void processElement2(EquityChange equity,
                                Context ctx,
                                Collector<CompanyWide> out) {
        CompanyWide wide = new CompanyWide();
        wide.setCompanyId(equity.getShareholderCreditCode());
        wide.setShareholderName(equity.getShareholderName());
        wide.setHoldRatio(equity.getRatio());
        wide.setEventTime(new Timestamp(equity.getEventTime()));
        wide.setOpType("");
        out.collect(wide);
    }
}
