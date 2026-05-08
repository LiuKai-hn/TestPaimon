package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:53
 */

import lombok.Data;

import java.sql.Timestamp;

@Data
public class CompanyInfo {
    private String companyId;      // 企业ID（主键）
    private String companyName;    // 企业名称
    private String legalPerson;    // 法人
    private String status;         // 经营状态
    private long regCapital;       // 注册资本
    private Timestamp updateTs;    // 更新时间戳
    private String opType;         // INSERT/UPDATE/DELETE
}