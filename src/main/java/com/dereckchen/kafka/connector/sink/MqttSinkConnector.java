package com.dereckchen.kafka.connector.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MqttSinkConnector extends SinkConnector {


    private static final Logger log = LoggerFactory.getLogger(MqttSinkConnector.class);

    private Map<String, String> configProps;

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} workers.", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }


    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef(); // non null
    }

    @Override
    public String version() {
        return "";
    }
}
