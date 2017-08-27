package dnkrn.storm.apache.logstats.topology.user;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dinakaran on 4/22/17.
 */
public class UserCountPerRequest extends BaseBasicBolt {

    private Map<String, Integer> counts;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String request = tuple.getStringByField("request");
        counts.put(request, countFor(request) + 1);
        printCounts();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map config,
                        TopologyContext context) {
        counts = new HashMap<String, Integer>();
    }


    private Integer countFor(String email) {
        Integer count = counts.get(email);
        return count == null ? 0 : count;
    }

    private void printCounts() {
        System.out.println("---------------------------------Printing Count------------------------------");
        for (String request : counts.keySet()) {
            System.out.println(
                    String.format("%s has count of %s", request, counts.get(request)));
        }
        System.out.println("-----------------------------END Printing Count------------------------------");

    }
}
