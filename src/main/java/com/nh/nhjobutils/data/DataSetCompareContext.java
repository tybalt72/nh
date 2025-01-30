package com.nh.nhjobutils.data;

import lombok.Builder;
import lombok.NonNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

@Builder
public class DataSetCompareContext {
    @NonNull private Collection<String> keyNames;
    @NonNull private Collection<String> attributes;
    @NonNull private Iterator<Map<String, Object>> sourceIterator;
    @NonNull private Iterator<Map<String, Object>> targetIterator;
    @NonNull private DataCompareConsumer consumer;

    public void compare() {
        int result;
        Map<String, Object> sourceRecord=null;
        Map<String, Object> targetRecord=null;
        if (sourceIterator.hasNext()) sourceRecord = sourceIterator.next();
        if (targetIterator.hasNext()) targetRecord = targetIterator.next();
        // 두 커서를 순차적으로 비교
        while (sourceRecord != null || targetRecord != null) {
            if (sourceRecord != null && targetRecord != null) {
                result = BatchUtils.compare(keyNames, sourceRecord, targetRecord);
                if (0==result) {
                    consumer.intersection(sourceRecord, targetRecord);
                    sourceRecord = sourceIterator.hasNext() ? sourceIterator.next() : null;
                    targetRecord = targetIterator.hasNext() ? targetIterator.next() : null;
                } else if (result < 0) {
                    consumer.sourceDifference(sourceRecord);
                    sourceRecord = sourceIterator.hasNext() ? sourceIterator.next() : null;
                } else {
                    consumer.targetDifference(targetRecord);
                    targetRecord = targetIterator.hasNext() ? targetIterator.next() : null;
                }
            } else if (sourceRecord != null) {
                consumer.sourceDifference(sourceRecord);
                sourceRecord = sourceIterator.hasNext() ? sourceIterator.next() : null;
            } else {
                consumer.targetDifference(targetRecord);
                targetRecord = targetIterator.hasNext() ? targetIterator.next() : null;
            }
        }
    }
}
