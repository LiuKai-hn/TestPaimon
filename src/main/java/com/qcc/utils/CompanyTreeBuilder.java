package com.qcc.utils;


/*
 *
 * @author liukai
 * @date 2026/5/8 16:43
 */

import com.qcc.pojo.CompanyNode;
import com.qcc.pojo.EquityPath;

import java.util.*;

public class CompanyTreeBuilder {

    public static CompanyNode buildTree(List<EquityPath> paths) {
        if (paths.isEmpty()) return null;

        String targetName = paths.get(0).getTargetCompanyName();
        CompanyNode root = new CompanyNode();
        root.setCompanyName(targetName);
        root.setRatio(1.0);

        Map<String, CompanyNode> map = new HashMap<>();
        map.put(targetName, root);

        for (EquityPath path : paths) {
            List<String> nodes = path.getPathNodes();
            double totalRatio = path.getTotalRatio();

            CompanyNode current = root;
            for (int i = 1; i < nodes.size(); i++) {
                String name = nodes.get(i);
                boolean exists = map.containsKey(name);

                CompanyNode node;
                if (exists) {
                    node = map.get(name);
                } else {
                    node = new CompanyNode();
                    node.setCompanyName(name);
                    map.put(name, node);
                }

                if (i == nodes.size() - 1) {
                    node.setRatio(totalRatio);
                }

                if (!contains(current.getShareholders(), name)) {
                    current.getShareholders().add(node);
                }

                current = node;
            }
        }
        return root;
    }

    private static boolean contains(List<CompanyNode> list, String name) {
        return list.stream().anyMatch(n -> n.getCompanyName().equals(name));
    }
}