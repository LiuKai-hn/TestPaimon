package com.qcc.func;


/*
 *
 * @author liukai
 * @date 2026/4/30 14:19
 */

import com.alibaba.fastjson2.JSON;
import com.qcc.comm.OutPutTagUtil;
import com.qcc.pojo.CompanyOrgOds;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class CompanyOdsProcessFunc extends ProcessFunction<String, CompanyOrgOds> {
    @Override
    public void processElement(String s, Context ctx, Collector<CompanyOrgOds> out) throws Exception {
        try {
            CompanyOrgOds ods = JSON.parseObject(s, CompanyOrgOds.class);
            out.collect(ods);
        }catch (Exception e){
            // 脏数据侧输出
            ctx.output(OutPutTagUtil.DIRTY_DATA_TAG, s);
        }
    }
}
