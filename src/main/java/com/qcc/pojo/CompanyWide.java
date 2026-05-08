package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:55
 */

import lombok.Data;

import java.sql.Timestamp;

@Data
public class CompanyWide {
    private String companyId;
    private String companyName;
    private String legalPerson;
    private String status;
    private long regCapital;
    private String shareholderName;
    private Double holdRatio;
    private String caseType;
    private Integer caseCount;
    private Timestamp eventTime;
    private String opType;
}
