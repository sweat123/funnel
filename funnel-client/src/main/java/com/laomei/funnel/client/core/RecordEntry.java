package com.laomei.funnel.client.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author laomei on 2018/7/29 9:24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecordEntry<T> {

    private T recotd;
}
