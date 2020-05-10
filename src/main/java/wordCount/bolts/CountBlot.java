package wordCount.bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.omg.PortableInterceptor.INACTIVE;

import java.util.HashMap;
import java.util.Map;

public class CountBlot extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String, Integer> counters;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters = new HashMap<String, Integer>();
        this.id = context.getThisTaskId();
        this.name = context.getThisComponentId();
    }

    @Override
    public void cleanup() {
        System.out.println("-- Word Counter Start [" + name + "-" + id + "] --");
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
        System.out.println("-- Word Counter End--");
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getString(0);
        if (!counters.containsKey(word)){
            counters.put(word, 1);
        }else {
            counters.put(word, counters.get(word) + 1);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
