package com.qcc.pojo;


import lombok.Data;

/*
 *ODS层：企业工商CDC原始流
 * @author liukai
 * @date 2026/4/30 14:24
 */
@Data
public class CompanyOrgOds {
    private String companyId;
    private String companyName;
    private String creditCode;
    private String industryCode;
    private String regAddr;
    private Integer enterpriseType;
    private Long eventTime;
    private String op;
    private String sourceTopic;
}
