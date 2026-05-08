package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:58
 */

import com.qcc.pojo.CompanyWide;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RiskWindowAggFunc extends ProcessWindowFunction<CompanyWide, CompanyWide, String, TimeWindow> {

    @Override
    public void process(String key,
                        Context context,
                        Iterable<CompanyWide> elements,
                        Collector<CompanyWide> out) {
        int totalCase = 0;
        CompanyWide result = null;
        for (CompanyWide wide : elements) {
            result = wide;
            if (wide.getCaseCount() != null) {
                totalCase += wide.getCaseCount();
            }
        }
        if (result != null) {
            result.setCaseCount(totalCase);
            out.collect(result);
        }
    }
}
