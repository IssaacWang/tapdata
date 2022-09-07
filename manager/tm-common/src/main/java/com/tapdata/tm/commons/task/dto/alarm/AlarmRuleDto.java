package com.tapdata.tm.commons.task.dto.alarm;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author jiuyetx
 * @date 2022/9/5
 */
@Data
@AllArgsConstructor
public class AlarmRuleDto {
    private String key;
    private int point;
    //-1 小于等于 <= ; 0; 1 大于等于 >=
    private int equalsFlag;
    //毫秒
    private int ms;
}
