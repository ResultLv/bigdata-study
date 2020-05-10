package wordCount.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;

    @Override
    public void ack(Object msgId) {
        System.out.println("OK " + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("Fail " + msgId);
    }

    /**
     * 打开文件 获取Collector对象
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            File file = new File("src/main/resources/words.txt");
            this.fileReader = new FileReader(file);
        }catch (FileNotFoundException e){
            throw new RuntimeException("Error reading file");
        }
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        if (completed){
            try {
                Thread.sleep(1000);
            }catch (InterruptedException e){

            }
        }
        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((str = reader.readLine()) != null){
                this.collector.emit(new Values(str), str);
            }
        }catch (Exception e){
            throw new RuntimeException("Error reading tuple",e);
        }finally {
            completed = true;
        }
    }

    /**
     * 声明输出的filed
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
