package dnkrn.storm.apache.logstats;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import dnkrn.storm.apache.logstats.topology.AccessLogListenerSpout;
import dnkrn.storm.apache.logstats.topology.user.UserCountExtractor;
import dnkrn.storm.apache.logstats.topology.user.UserCountPerRequest;

import java.util.concurrent.TimeUnit;

/**
 * Created by dinakaran on 4/16/17.
 */
public class LocalApacheLogsTopologyRunner {


    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("access-log-listener", new AccessLogListenerSpout());

        builder.setBolt("request-extractor", new UserCountExtractor())
               .shuffleGrouping("access-log-listener");

        builder.setBolt("request-counter", new UserCountPerRequest())
               .fieldsGrouping("request-extractor", new Fields("request"));

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("access-logs-topology",
                config,
                topology);
       // TimeUnit.MINUTES.sleep(2);
       // cluster.killTopology("github-apache-logs-topology");
       // cluster.shutdown();
    }
}
