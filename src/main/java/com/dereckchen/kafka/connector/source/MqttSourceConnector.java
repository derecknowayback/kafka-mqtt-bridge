package com.dereckchen.kafka.connector.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MqttSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MqttSourceConnector.class);

    private Map<String,String> props;

    private final ConfigDef CONF = new ConfigDef();

    @Override
    public void start(Map<String, String> map) {
        this.props = map;
        log.info("Starting MqttSourceConnector");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MqttSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return Collections.singletonList(props);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return CONF;
    }

    @Override
    public String version() {
        return "";
    }
}
