package com.dereckchen.kafka.connector.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.dereckchen.util.MQTTUtil.MQTTConfig;

public class MqttSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MqttSourceTask.class);

    private Map<String, String> props;
    private String kafkaTopic;
    private AtomicBoolean running;
    private MqttClient client;
    private ArrayDeque<SourceRecord> records;

    @Override
    public String version() {
        return "";
    }

    @Override
    public void start(Map<String, String> map) {
        props = map;
        kafkaTopic = map.get("topic");
        initializeMqttClient();
        running = new AtomicBoolean(true);
        records = new ArrayDeque<>(10);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (records.isEmpty()) {
            return null;
        }
        List<SourceRecord> sourceRecords = new ArrayList<>(records);
        records.clear();
        return sourceRecords;
    }


    public void initializeMqttClient() {
        try {

            MQTTConfig mqttConfig = parseConfig(props);
            client = new MqttClient(mqttConfig.getBroker(), mqttConfig.getClientid(), new MemoryPersistence());

            // 连接参数
            MqttConnectOptions options = new MqttConnectOptions();
            options.setUserName(mqttConfig.getUsername());
            options.setPassword(mqttConfig.getPassword().toCharArray());
            options.setConnectionTimeout(0);
            options.setKeepAliveInterval(0);
            options.setAutomaticReconnect(false);


            // 设置回调
            client.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    log.error("connectionLost: {}", cause.getMessage(),cause);

                    // 尝试重连
                    while (running.get()) {
                        try {
                            log.info("尝试重连中。。。");
                            client.connect(options);
                            client.subscribe(mqttConfig.getTopic(), 0);
                            log.info("尝试重连成功。。。");
                            break;
                        } catch (Exception e) {
                            log.error("重连失败", e);
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException ex) {
                                log.error("Wait time exception...",ex);
                            }
                        }
                    }
                }

                public void messageArrived(String topic, MqttMessage message) {
                    log.info("Topic [{}] incoming msg: {}", topic, new String(message.getPayload()));
                    byte[] payload = message.getPayload();
                    records.add(new SourceRecord(null, null, kafkaTopic, (Integer) null, (Schema) null, (Object) null, Schema.STRING_SCHEMA, payload));
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    log.info("deliveryComplete---------{}", token.isComplete());
                }
            });

            client.connect(options);
            log.info("Start listening to topics {}", mqttConfig.getTopic());
            client.subscribe(mqttConfig.getTopic(), 0);
            client.publish(mqttConfig.getTopic(), "hello world".getBytes(), 0, false);
            log.info("Successfully publish hello world");
        } catch (Exception e) {
            log.error("Error initializing MQTT client", e);
            throw new RuntimeException(e);
        }
    }

    public MQTTConfig parseConfig(Map<String, String> props) {
        return MQTTConfig.builder()
                .password(props.getOrDefault("mqtt.password", ""))
                .clientid(props.getOrDefault("mqtt.clientid", ""))
                .username(props.getOrDefault("mqtt.username", ""))
                .broker(props.getOrDefault("mqtt.broker", ""))
                .topic(props.getOrDefault("mqtt.topic", "")).build();
    }

    @Override
    public void stop() {
        try {
            client.disconnect();
            running.set(false);
        } catch (MqttException e) {
            log.error("Close mqttClient failed...");
            throw new RuntimeException(e);
        }
        log.info("Stopped MQTT Source Task");
    }
}
