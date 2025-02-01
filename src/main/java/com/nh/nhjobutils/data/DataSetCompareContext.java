package com.nh.nhjobutils.data;

import jakarta.annotation.Nullable;
import lombok.Builder;
import lombok.NonNull;
import org.springframework.batch.item.ItemReader;

import java.util.Collection;
import java.util.Map;

@Builder
public class DataSetCompareContext {
    @NonNull private Collection<String> keyNames;
    @NonNull private Collection<String> attributes;
    @NonNull private ItemReader<Map<String, Object>> sourceReader;
    @NonNull private ItemReader<Map<String, Object>> targetReader;
    @NonNull private DataCompareConsumer consumer;

    public void compare() throws Exception {
        int result;
        @Nullable
        Map<String, Object> sourceRecord=sourceReader.read();
        @Nullable
        Map<String, Object> targetRecord=targetReader.read();
        // 두 커서를 순차적으로 비교
        while (true) {
            if (sourceRecord != null && targetRecord != null) {
                result = BatchUtils.compare(keyNames, sourceRecord, targetRecord);
                if (0==result) {
                    consumer.intersection(sourceRecord, targetRecord);
                    sourceRecord = sourceReader.read();
                    targetRecord = targetReader.read();
                } else if (result < 0) {
                    consumer.sourceDifference(sourceRecord);
                    sourceRecord = sourceReader.read();
                } else {
                    consumer.targetDifference(targetRecord);
                    targetRecord = targetReader.read();
                }
            } else if (sourceRecord != null) {
                consumer.sourceDifference(sourceRecord);
                sourceRecord = sourceReader.read();
            } else if (targetRecord != null) {
                consumer.targetDifference(targetRecord);
                targetRecord = targetReader.read();
            } else {
                break;
            }
        }
    }
}
