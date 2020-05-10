package wordCount.bolts;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitterBlot extends BaseBasicBolt {

    /**
     * 将接收到的行tuple拆分为单词再发送至下一个Bolt
     * @param input
     * @param collector
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for (String word : words){
            word = word.trim();
            if (StringUtils.isNotBlank(word)){
                collector.emit(new Values(word.toLowerCase()));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
