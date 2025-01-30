package com.nh.nhjobutils.data;

import org.springframework.batch.item.ItemReader;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class BatchUtils {
    public static Iterator<Map<String,Object>> iterator(ItemReader<Map<String, Object>> itemReader) {
        return new Iterator<>() {
            private Map<String, Object> nextItem;

            @Override
            public boolean hasNext() {
                try {
                    if (nextItem == null) {
                        nextItem = itemReader.read();
                    }
                    return nextItem != null;
                } catch (Exception e) {
                    throw new RuntimeException("Error reading data", e);
                }
            }

            @Override
            public Map<String, Object> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Map<String, Object> result = nextItem;
                nextItem = null;
                return result;
            }
        };
    }
    public static int compare(Collection<String> keyNames, Map<String, Object> a, Map<String, Object> b) {
        int result=0;
        for (String key : keyNames) {
            Object valueA = a.get(key);
            Object valueB = b.get(key);

            // Null 체크
            if (valueA == null && valueB == null) {
                continue;
            } else if (valueA == null) {
                return -1; // null 값은 항상 작은 값으로 간주
            } else if (valueB == null) {
                return 1; // null 값은 항상 작은 값으로 간주
            }

            // Comparable을 사용한 비교
            if (valueA instanceof Comparable && valueB instanceof Comparable) {
                result = ((Comparable<Object>) valueA).compareTo(valueB);
            } else {
                // Comparable이 아닌 경우 String으로 변환 후 비교
                result = valueA.toString().compareTo(valueB.toString());
            }
            if (result != 0) {
                break;
            }
        }
        return result; 
    }
}
