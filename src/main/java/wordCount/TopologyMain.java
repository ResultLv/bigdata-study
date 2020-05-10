package wordCount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import wordCount.bolts.CountBlot;
import wordCount.bolts.SplitterBlot;
import wordCount.spouts.WordReader;

public class TopologyMain {

    public static void main(String[] args) throws InterruptedException{
        //拓扑定义
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WordReader());
        builder.setBolt("split", new SplitterBlot())
                .shuffleGrouping("spout");
        builder.setBolt("count", new CountBlot(), 1)
                .fieldsGrouping("split", new Fields("word"));

        //配置
        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        conf.setMaxTaskParallelism(3);

        //本地Cluster启动拓扑
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
