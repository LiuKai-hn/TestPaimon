package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/4/30 11:54
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EquityChange {
    private String shareholderCreditCode;
    private String shareholderName;
    private String investeeCreditCode;
    private String investeeName;
    private Double ratio;
    private Long eventTime;
}