package com.example.kafka.connect.iidr;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Kafka Connect Sink Connector for IIDR CDC events.
 *
 * This connector processes CDC events from IIDR using the A_ENTTYP header
 * to determine the operation type (INSERT, UPDATE, DELETE) and writes to a target
 * JDBC database.
 */
public class IidrCdcSinkConnector extends SinkConnector {

    private static final Logger log = Logger.getLogger(IidrCdcSinkConnector.class.getName());

    public static final String VERSION = "1.0.0";

    private Map<String, String> configProps;

    @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting IidrCdcSinkConnector");
        this.configProps = props;

        // Validate configuration
        new IidrCdcSinkConfig(props);

        log.info("IidrCdcSinkConnector started with configuration: connection.url=" +
                props.get(IidrCdcSinkConfig.CONNECTION_URL_CONFIG) +
                ", corrupt.events.table=" +
                props.getOrDefault(IidrCdcSinkConfig.CORRUPT_EVENTS_TABLE_CONFIG,
                        IidrCdcSinkConfig.CORRUPT_EVENTS_TABLE_DEFAULT) +
                ", default.timezone=" +
                props.getOrDefault(IidrCdcSinkConfig.DEFAULT_TIMEZONE_CONFIG,
                        IidrCdcSinkConfig.DEFAULT_TIMEZONE_DEFAULT));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IidrCdcSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating " + maxTasks + " task configurations");

        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            configs.add(taskConfig);
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping IidrCdcSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return IidrCdcSinkConfig.CONFIG_DEF;
    }
}
