package com.nh.nhjobutils.data;

import java.util.Map;

public interface DataCompareConsumer {
    default void intersection(Map<String, Object> source, Map<String, Object> target) { }
    default void sourceDifference(Map<String, Object> source) { }
    default void targetDifference(Map<String, Object> source) { }
}
