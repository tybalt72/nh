package com.nh.nhjobutils;

import com.nh.nhjobutils.data.DataCompareConsumer;
import com.nh.nhjobutils.data.DataSetCompareContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SpringBootTest
@Slf4j
class NhJobUtilsApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    void dataSetBuilderTest() throws Exception {
        List<String> keys = Stream.of("k1", "k2").toList();
        List<String> vals = Stream.of("v1", "v2").toList();
        List<String> cols = Stream.concat(keys.stream(), vals.stream()).toList();
        DataSetCompareContext builder = DataSetCompareContext.builder()
                .keyNames(keys)
                .attributes(vals)
                .sourceReader(dataReader(dataIterator(cols, 3, 5, 8)))
                .targetReader(dataReader(dataIterator(cols, 2, 1, 6)))
                .consumer(new DataCompareConsumer() {
                    @Override
                    public void intersection(Map<String, Object> source, Map<String, Object> target) {
                        log.info("교집합 : {}/{}", keyValuesString(keys, source), keyValuesString(keys, target) );
                    }
                    @Override
                    public void sourceDifference(Map<String, Object> source) {
                        log.info("차집합 : source only {}", keyValuesString(keys, source) );
                    }
                    @Override
                    public void targetDifference(Map<String, Object> source) {
                        log.info("차집합 : target only {}", keyValuesString(keys, source) );
                    }
                })
                .build();
        builder.compare();
    }
    private String keyValuesString(List<String> keys, Map<String, Object> record) {
        return keys.stream()
                .map(record::get)
                .map(Object::toString)
                .collect(Collectors.joining(":"));
    }
    private Iterator<Map<String, Object>> dataIterator(List<String> cols, int interval, int begin, int limit) {
        return IntStream.iterate(begin, val -> val + interval)
                .limit(limit)
                .mapToObj(seed -> cols.stream().collect(Collectors.toMap(Function.identity(), xx -> (Object)("val-" + seed))))
                .iterator();
    }
    private ItemReader<Map<String,Object>> dataReader(Iterator<Map<String, Object>> iterator) {
        ItemReader<Map<String,Object>> reader = new ItemReader<Map<String, Object>>() {
            @Override
            public Map<String, Object> read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                return iterator.hasNext() ? iterator.next() : null;
            }
        };
        return reader;
    }
}
