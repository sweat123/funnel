package com.laomei.funnel.client.util;

import com.laomei.funnel.client.core.TimeBucket;

/**
 * @author laomei on 2018/7/28 9:14
 */
public class BucketUtil {

    /**
     * create an new TimeBucket with current timestamp and bucket interval
     * @param currentTimestamp current timestamp
     * @param bucketInterval time bucket interval
     * @return new timeBucket
     */
    public static TimeBucket newBucket(long currentTimestamp, long bucketInterval) {
        long begin = currentTimestamp - currentTimestamp % bucketInterval;
        long end = currentTimestamp + bucketInterval;
        return new TimeBucket(begin, end);
    }
}
