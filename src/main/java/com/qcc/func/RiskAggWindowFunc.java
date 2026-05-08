package com.qcc.func;


/*
 *滑动窗口风险聚合
 * @author liukai
 * @date 2026/4/30 14:21
 */
import com.qcc.pojo.CompanyRiskDwd;
import com.qcc.pojo.CompanyRiskDws;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RiskAggWindowFunc extends ProcessWindowFunction<CompanyRiskDwd, CompanyRiskDws, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<CompanyRiskDwd> iterable, Collector<CompanyRiskDws> out) {
        CompanyRiskDws dws = new CompanyRiskDws();
        int totalPunish = 0;
        double totalMoney = 0;
        String industry = "";
        for (CompanyRiskDwd dwd : iterable) {
            totalPunish += dwd.getPunishCount();
            totalMoney += dwd.getTotalPunishAmount();
            industry = dwd.getIndustryName();
        }
        dws.setCompanyId(s);
        dws.setIndustryName(industry);
        dws.setPunishTotal(totalPunish);
        dws.setPunishTotalMoney(totalMoney);
        // 风险等级打分
        if(totalPunish >= 5){
            dws.setRiskLevel(3);
        }else if(totalPunish >=2){
            dws.setRiskLevel(2);
        }else {
            dws.setRiskLevel(1);
        }
        dws.setWindowStartTime(context.window().getStart());
        dws.setWindowEndTime(context.window().getEnd());
        dws.setStatTime(System.currentTimeMillis());
        out.collect(dws);
    }
}
