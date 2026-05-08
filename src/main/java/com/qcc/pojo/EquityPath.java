package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/5/6 17:00
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EquityPath {
    private String controllerCode;
    private String controllerName;
    private String targetCompanyCode;
    private String targetCompanyName;
    private Double totalRatio;
    private Integer depth;
    private List<String> pathNodes;
    private Boolean isUltimateBeneficiary;
    private Long eventTime;
}