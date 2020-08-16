package flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Result Lv
 * @date 2020/8/16 5:50 下午
 */
public class BatchWordCount {

    // 创建Flink运行的上下文环境
    private static final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        DataSource<String> dataSource = env.readTextFile("src/main/resources/words.txt");

        DataSet<Tuple2<String, Integer>> counts = dataSource.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        counts.printToErr();
    }
}
