package com.qcc.pojo;


import lombok.Data;

/*
 *
 * @author liukai
 * @date 2026/4/30 14:22
 * 股权穿透递归结果
 */
@Data
public class EquityTreeDto {
    private String rootCompanyId;
    private String currentCompanyId;
    private String currentName;
    private Double totalHoldRatio;
    private Integer treeLevel; // 穿透层级
    private Long calcTime;
}