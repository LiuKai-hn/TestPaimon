package com.qcc.pojo;


import lombok.Data;

/*
 *DWD层：企业风险明细宽表（双流Join结果）
 * @author liukai
 * @date 2026/4/30 14:22
 */
@Data
public class CompanyRiskDwd {
    private String companyId;
    private String companyName;
    private String creditCode;
    private String industryName;
    private Integer punishCount;
    private Double totalPunishAmount;
    private String latestPunishType;
    private Long businessUpdateTime;
    private Long riskUpdateTime;
    private String dataModifyFlag; // normal/retract 回撤标记
}
