package com.laomei.funnel.client.core;

import com.laomei.funnel.common.data.TimeBucket;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author laomei on 2018/8/12 20:17
 */
public class ConsumerAuditor<K, V> {

    private Map<Long, TimeBucket> timeBucketMap;

    private final long timeBucketInterval;

    private final AtomicBoolean needReport;

    private Set<TimeBucket> waitReportedTimeBuckets;

    public ConsumerAuditor(long timeBucketInterval) {
        this.timeBucketInterval = timeBucketInterval;
        this.timeBucketMap = new HashMap<>();
        this.needReport = new AtomicBoolean(false);
        this.waitReportedTimeBuckets = new HashSet<>();
    }

    public boolean audit(ConsumerRecord<K, V> record) {
        if (!isTriggerRecord(record)) {
            long currentTimestamp = System.currentTimeMillis();
            TimeBucket timeBucket = getTimeBucket(currentTimestamp);
            long recordTimestamp = record.timestamp();
            long latency = currentTimestamp - recordTimestamp;
            timeBucket.getStats().addValue(latency);
            timeBucket.setMsgCount(timeBucket.getMsgCount() + 1);
        }
        return needReport.getAndSet(false);
    }

    public Set<TimeBucket> getAndResetWaitReportedTimeBuckets() {
        Set<TimeBucket> timeBuckets = new HashSet<>(waitReportedTimeBuckets);
        waitReportedTimeBuckets = new HashSet<>();
        return timeBuckets;
    }

    private boolean isTriggerRecord(ConsumerRecord<K, V> record) {
        return record.key() == null
                && record.value() == null
                && record.offset() == -1
                && record.partition() == -1;
    }

    private TimeBucket getTimeBucket(long timestamp) {
        long bucketBegin = getTimeBucketBegin(timestamp);
        TimeBucket timeBucket = timeBucketMap.get(bucketBegin);
        if (timeBucket == null) {
            resetTimeBucketMap();
            timeBucket = new TimeBucket(bucketBegin, bucketBegin + timeBucketInterval);
            timeBucketMap.put(bucketBegin, timeBucket);
        }
        return timeBucket;
    }

    private void resetTimeBucketMap() {
        needReport.set(true);
        waitReportedTimeBuckets.addAll(timeBucketMap.values());
        timeBucketMap = new HashMap<>();
    }

    private long getTimeBucketBegin(long timestamp) {
        return timestamp - timestamp % timeBucketInterval;
    }
}
