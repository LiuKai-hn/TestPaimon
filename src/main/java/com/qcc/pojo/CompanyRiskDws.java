package com.qcc.pojo;


import lombok.Data;

/*
 *
 * @author liukai
 * @date 2026/4/30 14:22
/**
 * DWS层：企业风险聚合指标
 */
@Data
public class CompanyRiskDws {
    private String companyId;
    private String industryName;
    private Integer riskLevel; // 1低 2中 3高
    private Integer punishTotal;
    private Double punishTotalMoney;
    private Long windowStartTime;
    private Long windowEndTime;
    private Long statTime;
}
