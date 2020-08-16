package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author Result Lv
 * @date 2020/8/16 6:31 下午
 */
public class StreamLineSplitter {

    // 创建Flink的流式计算环境
//    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 监听本地7777端口
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 7777);
        // 将接收的数据进行拆分，分组，窗口计算并且进行聚合输出
        DataStream<WordWithCount> counts = streamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                for (String word : value.split("\\s")) {
                    collector.collect(new WordWithCount(word, 1));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount w1, WordWithCount w2) throws Exception {
                        System.out.println(w1.count + "  " + w2.count);
                        return new WordWithCount(w1.word, w1.count + w2.count);
                    }
                });

        counts.print();
        //执行监听与计算
        env.execute("Flink Streaming Word Count By Java");
    }

//    public static void main(String[] args) throws Exception {
//
//        // 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 设置socket数据源
//        DataStreamSource<String> source = env.socketTextStream("localhost", 7777, "\n");
//        // 转化处理数据
//        DataStream<WordWithCount> dataStream = source.flatMap(new FlatMapFunction<String, WordWithCount>() {
//            @Override
//            public void flatMap(String line, Collector<WordWithCount> collector) throws Exception {
//                for (String word : line.split(" ")) {
//                    collector.collect(new WordWithCount(word, 1));
//                }
//            }
//        }).keyBy("word")//以key分组统计
//                .timeWindow(Time.seconds(20),Time.seconds(20))//设置一个窗口函数，模拟数据流动
//                .sum("count");//计算时间窗口内的词语个数
//
//        // 输出数据到目的端
//        dataStream.print();
//
//        // 执行任务操作
//        env.execute("Flink Streaming Word Count By Java");
//
//    }

    public static class WordWithCount{
        public String word;
        public int count;

        public WordWithCount(){

        }

        public WordWithCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
