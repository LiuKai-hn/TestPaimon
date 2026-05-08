package com.liukai.pojo;


/*
 *
 * @author liukai
 * @date 2026/4/23 14:44
 */


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// =========================================
// Model 定义
// =========================================
@Data
public class UniformMessage {
    private Integer userId;
    private String name;
    private String level;
    private String city;

    public UniformMessage(Integer userId, String name, String level, String city) {
        this.userId = userId;
        this.name = name;
        this.level = level;
        this.city = city;
    }
}

