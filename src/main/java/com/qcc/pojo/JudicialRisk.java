package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:54
 */

import lombok.Data;

import java.sql.Timestamp;

@Data
public class JudicialRisk {
    private String companyId;
    private String caseType;       // 案件类型
    private Integer caseCount;      // 涉案数量
    private Timestamp publishTs;
    private String opType;
}
