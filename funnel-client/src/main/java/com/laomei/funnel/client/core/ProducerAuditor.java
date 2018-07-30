package com.laomei.funnel.client.core;

import lombok.val;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author laomei on 2018/7/29 15:57
 */
public class ProducerAuditor<K, V> {

    private final long timeBucketInterval;

    private final AtomicBoolean needReport;

    private Map<Long, TimeBucket> timeBucketMap;

    private Set<TimeBucket> waitReportedTimeBuckets = new HashSet<>();

    public ProducerAuditor(long timeBucketInterval) {
        this.timeBucketInterval = timeBucketInterval;
        this.timeBucketMap = new ConcurrentHashMap<>();
        this.needReport = new AtomicBoolean(false);
    }

    public boolean audit(ProducerRecord<K, V> record) {
        val currentTimestamp = System.currentTimeMillis();
        val timeBucket = getTimeBucket(currentTimestamp);
        if (!isTriggerRecord(record)) {
            timeBucket.setMsgCount(timeBucket.getMsgCount() + 1);
        }
        return needReport.getAndSet(false);
    }

    public Set<TimeBucket> getAndResetWaitReportedTimeBuckets() {
        val timeBuckets = new HashSet<TimeBucket>(waitReportedTimeBuckets);
        waitReportedTimeBuckets = new HashSet<>();
        return timeBuckets;
    }

    /**
     * if record is trigger record return true, else false;
     *
     * trigger record is used for triggering any time bucket need report;
     *
     */
    private boolean isTriggerRecord(ProducerRecord<K, V> record) {
        return record == null;
    }

    private TimeBucket getTimeBucket(long timestamp) {
        val bucketBegin = getTimeBucketBegin(timestamp);
        TimeBucket timeBucket = timeBucketMap.get(bucketBegin);
        if (timeBucket == null) {
            //new bucket is created, so buckets which begin time is before current bucket need to be reported;
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
