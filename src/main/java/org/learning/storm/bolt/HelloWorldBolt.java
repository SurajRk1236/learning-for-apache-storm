package org.learning.storm.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static org.learning.storm.constants.Constant.MESSAGE;

public class HelloWorldBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Integer requiredValue = tuple.getInteger(0);
        requiredValue *= 2;
        System.out.println("VALUE PROCESSED in storm topology :: " + requiredValue);
        basicOutputCollector.emit(new Values(requiredValue));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(MESSAGE));
    }
}
