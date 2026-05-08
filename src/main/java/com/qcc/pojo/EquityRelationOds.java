package com.qcc.pojo;


import lombok.Data;

/*
 * ODS层：股权关系原始流
 * @author liukai
 * @date 2026/4/30 14:36
 */
@Data
public class EquityRelationOds {
    private String parentId;
    private String childId;
    private String parentName;
    private String childName;
    private Double holdRatio;
    private Long relationTime;
}
