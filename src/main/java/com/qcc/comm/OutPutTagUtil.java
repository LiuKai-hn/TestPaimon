package com.qcc.comm;


/*
 * // ################### 侧输出：脏数据、迟到数据、告警分流 ###################
 * @author liukai
 * @date 2026/4/30 14:41
 */

import com.qcc.pojo.CompanyRiskDwd;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.OutputTag;

public class OutPutTagUtil {

    public static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty_data"){};
    public static final OutputTag<CompanyRiskDwd> RISK_ALERT_TAG = new OutputTag<CompanyRiskDwd>("risk_alert"){};
    // ################### 广播维表描述符：行业字典 ###################
    public static final MapStateDescriptor<String, String> INDUSTRY_BROADCAST_DESC =
            new MapStateDescriptor<>("industry_dict", Types.STRING, Types.STRING);

}
