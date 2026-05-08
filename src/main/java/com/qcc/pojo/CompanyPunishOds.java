package com.qcc.pojo;


import lombok.Data;

import java.io.Serializable;

/*
 *ODS层：行政处罚原始流
 * @author liukai
 * @date 2026/4/30 14:24
 */
@Data
public class CompanyPunishOds implements Serializable {
    private String companyId;
    private String punishNo;
    private String punishType;
    private Double punishMoney;
    private Long punishTime;
    private String op;
}
