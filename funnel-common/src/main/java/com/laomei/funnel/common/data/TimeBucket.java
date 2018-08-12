package com.laomei.funnel.common.data;

import lombok.Data;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * @author laomei on 2018/7/27 22:10
 */
@Data
public class TimeBucket {

    private long begin;

    private long end;

    private long msgCount;

    private DescriptiveStatistics stats;

    public TimeBucket(long begin, long end) {
        this.begin = begin;
        this.end = end;
        this.msgCount = 0;
        stats = new DescriptiveStatistics(-1);
    }
}
