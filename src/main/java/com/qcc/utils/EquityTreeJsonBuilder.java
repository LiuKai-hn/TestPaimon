package com.qcc.utils;


/*
 *
 * @author liukai
 * @date 2026/5/8 16:40
 */

import com.qcc.pojo.CompanyHierarchy;

import java.util.List;

public class EquityTreeJsonBuilder {

    // 把 pathNodes 列表 → 嵌套 parent 结构
    public static CompanyHierarchy buildTree(List<String> pathNodes, double totalRatio) {
        if (pathNodes == null || pathNodes.isEmpty()) {
            return null;
        }

        // 最底层公司（例如 A公司）
        CompanyHierarchy root = new CompanyHierarchy();
        root.setCompanyName(pathNodes.get(0));
        root.setRatio(1.0);

        CompanyHierarchy current = root;

        // 逐层往上套 parent
        for (int i = 1; i < pathNodes.size(); i++) {
            CompanyHierarchy parent = new CompanyHierarchy();
            parent.setCompanyName(pathNodes.get(i));
            parent.setRatio(i == pathNodes.size() - 1 ? totalRatio : null);

            current.setParent(parent);
            current = parent;
        }

        return root;
    }
}
