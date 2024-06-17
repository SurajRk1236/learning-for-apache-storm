package org.learning.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.learning.storm.bolt.HelloWorldBolt;
import org.learning.storm.spout.HelloWorldSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.learning.storm.constants.Constant.*;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        System.out.println("Building Storm Topology!");
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(HELLO_WORLD_SPOUT, new HelloWorldSpout());
        builder.setBolt(HELLO_WORLD_BOLT, new HelloWorldBolt()).shuffleGrouping(HELLO_WORLD_SPOUT);

        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster localCluster = new LocalCluster();
        try {
            localCluster.submitTopology(HELLO_WORLD_TOPOLOGY, conf, builder.createTopology());
            Thread.sleep(1000000);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }finally {
            localCluster.shutdown();
        }
    }
}