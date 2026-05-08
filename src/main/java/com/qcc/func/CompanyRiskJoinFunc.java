package com.qcc.func;


/*
 *双流KeyedCoProcess 复杂Join + 状态保存历史处罚数据
 * @author liukai
 * @date 2026/4/30 14:18
 */

import com.qcc.pojo.CompanyOrgOds;
import com.qcc.pojo.CompanyPunishOds;
import com.qcc.pojo.CompanyRiskDwd;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class CompanyRiskJoinFunc extends KeyedCoProcessFunction<String, CompanyOrgOds, CompanyPunishOds, CompanyRiskDwd> {
    private ListState<CompanyPunishOds> punishState;

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<CompanyPunishOds> desc =
                new ListStateDescriptor<>("punish_list", CompanyPunishOds.class);
        // 状态TTL 180天，大厂冷热数据分离
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(180))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        desc.enableTimeToLive(ttlConfig);
        punishState = getRuntimeContext().getListState(desc);
    }

    @Override
    public void processElement1(CompanyOrgOds orgOds, Context ctx, Collector<CompanyRiskDwd> out) throws Exception {
        CompanyRiskDwd dwd = new CompanyRiskDwd();
        dwd.setCompanyId(orgOds.getCompanyId());
        dwd.setCompanyName(orgOds.getCompanyName());
        dwd.setCreditCode(orgOds.getCreditCode());
        dwd.setBusinessUpdateTime(orgOds.getEventTime());
        // 读取状态中处罚数据
        Iterable<CompanyPunishOds> punishList = punishState.get();
        int count = 0;
        double money = 0;
        for (CompanyPunishOds punish : punishList) {
            count ++;
            money += punish.getPunishMoney();
        }
        dwd.setPunishCount(count);
        dwd.setTotalPunishAmount(money);
        dwd.setDataModifyFlag("normal");
        out.collect(dwd);
    }

    @Override
    public void processElement2(CompanyPunishOds punishOds, Context ctx, Collector<CompanyRiskDwd> out) throws Exception {
        // 处罚数据存入状态
        punishState.add(punishOds);
        // 触发回撤更新
        CompanyRiskDwd retract = new CompanyRiskDwd();
        retract.setCompanyId(punishOds.getCompanyId());
        retract.setDataModifyFlag("retract");
        out.collect(retract);
    }
}
