package com.laomei.funnel.common.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * @author laomei on 2018/7/29 13:46
 */
public class RecordEventFactory<T> implements EventFactory<RecordEntry<T>> {
    @Override
    public RecordEntry<T> newInstance() {
        return new RecordEntry<>(null);
    }
}
