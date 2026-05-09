package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/5/9 11:56
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 企查查 统一标准实体
 * 解决 Union 核心痛点：多流 Schema 不统一
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QccStandardEntity implements Serializable {
    // 主键
    private String id;
    // 企业统一信用代码
    private String companyCode;
    // 企业名称
    private String companyName;
    // 数据类型：1-工商 2-风险 3-舆情 4-关联关系
    private Integer dataType;
    // 内容
    private String content;
    // 时间戳
    private Long ts;
}
