package cn.liyohe.kafka.j60870.source;

import cn.liyohe.kafka.j60870.common.ConfigConstant;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.*;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * j60870源连接器
 *
 * @author liyohe
 * @date 2022/11/16
 */
public class J60870SourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(J60870SourceConnector.class);

    private Map<String, String> config;


    @Override
    public void start(Map<String, String> props) {
        config = props;
        for (Map.Entry<String, String> entry : props.entrySet()) {
            logger.info("****************************" + entry.getKey() + "  :  " + entry.getValue());
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return J60870SourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("------------maxTasks is " + maxTasks);
        if (maxTasks != 1) {
            logger.warn("当前connector不支持多任务！");
        }
        return Collections.singletonList(config);
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(ConfigConstant.TOPIC, Type.STRING, Importance.HIGH, "The topic to publish data to.")
                .define(ConfigConstant.SERVER_HOST, Type.STRING, Importance.HIGH, "IEC 104 server address.")
                .define(ConfigConstant.SERVER_PORT, Type.INT, 2404, Importance.HIGH, "IEC 104 server port.")
                .define(ConfigConstant.CONNECT_TIMEOUT, Type.LONG, 5000, Importance.LOW, "The connect time out (ms).")
                .define(ConfigConstant.ALL_CALL_CYCLE, Type.LONG, 15, Importance.HIGH, "The all call cycle (m).");
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}
