package flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 文本分割
 * @author Result Lv
 * @date 2020/8/16 6:10 下午
 */
public class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] splits = value.toLowerCase().split("\\n");
        for (String split : splits){
            if (StringUtils.isNoneBlank(split)){
                collector.collect(new Tuple2<String, Integer>(split, 1));
            }
        }
    }
}
