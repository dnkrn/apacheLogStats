package dnkrn.storm.apache.logstats.topology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.rabbitmq.client.*;
import dnkrn.storm.apache.logstats.domain.AccessLogRecord;
import dnkrn.storm.apache.logstats.services.AccessLogParser;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/**
 * Created by dinakaran on 4/22/17.
 */
public class AccessLogListenerSpout implements IRichSpout {

    private static final Logger log = Logger.getLogger(AccessLogListenerSpout.class.getName());


    private transient Connection connection;
    private transient Channel channel;
    private transient DefaultConsumer consumer;
    private transient String consumerTag;

    private SpoutOutputCollector outputCollector;
    private Integer prefetchCount;
    private final String QUEUE_NAME = "apacheLogs";
    private static final String CONFIG_PREFETCH_COUNT = "amqp.prefetch.count";
    private static final Long DEFAULT_PREFETCH_COUNT = 20L;
    private transient AccessLogParser accessLogParser;

    private String logLine;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = outputCollector;
        accessLogParser = new AccessLogParser();
        Long prefetchCount = (Long) map.get(CONFIG_PREFETCH_COUNT);
        if (prefetchCount == null) {
            prefetchCount = DEFAULT_PREFETCH_COUNT;
        } else if (prefetchCount < 1) {
            throw new IllegalArgumentException(CONFIG_PREFETCH_COUNT + "must be at least 1");
        }
        this.prefetchCount = prefetchCount.intValue();

        try {
            setupAMQP();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void reconnect() throws Exception {
        setupAMQP();
    }

    @Override
    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }


    private void setupAMQP() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("admin");
        factory.setPassword("admin");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(DEFAULT_PREFETCH_COUNT.intValue());




    }


    @Override
    public void nextTuple() {
        try {
            consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    logLine = new String(body, "UTF-8");
                    System.out.println(" [x] Received '" + logLine + "'");
                    Optional<AccessLogRecord> accessLogRecord = accessLogParser.parseRecord(logLine);
                    if (accessLogRecord.isPresent()) {
                        outputCollector.emit(new Values(accessLogRecord.get()));
                    }

                }
            };
            channel.basicConsume(QUEUE_NAME, true, consumer);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object msgId) {
        try {
            channel.basicAck((Long) msgId, false /* only acking this msgId */);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void fail(Object msgId) {
        try {
            channel.basicReject((Long) msgId, true /* requeue enabled */);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("accessLogRecord"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
