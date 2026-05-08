package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/5/7 19:24
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompanyHierarchy {
    private String companyCode;    // 企业/自然人Code
    private String companyName;    // 名称
    private Double ratio;          // 持股比例
    private CompanyHierarchy parent; // 上层股东（嵌套）

    // 工具方法：把整个嵌套结构 → 1个大JSON字符串
    public String toJson() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (Exception e) {
            return "{}";
        }
    }
}