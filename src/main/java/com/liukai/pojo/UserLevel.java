package com.liukai.pojo;


/*
 *
 * @author liukai
 * @date 2026/4/23 15:16
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserLevel {
    private Integer userId;
    private String level;
}
