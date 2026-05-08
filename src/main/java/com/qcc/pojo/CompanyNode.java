package com.qcc.pojo;


/*
 *
 * @author liukai
 * @date 2026/5/8 16:43
 */

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class CompanyNode {
    private String companyCode;
    private String companyName;
    private Double ratio;
    private List<CompanyNode> shareholders = new ArrayList<>();
}
