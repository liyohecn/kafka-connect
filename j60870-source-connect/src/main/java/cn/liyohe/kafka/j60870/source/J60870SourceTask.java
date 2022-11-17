package cn.liyohe.kafka.j60870.source;

import cn.liyohe.kafka.j60870.common.ConfigConstant;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class J60870SourceTask extends SourceTask {

    // 值的数据格式
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    // 偏移量字段
    public static final String POSITION_FIELD = "position";


    // 保存当前偏移量
    private Long position = null;

    private String topic;

    private Long timestamp = 0L;

    @Override
    public String version() {
        return new J60870SourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(ConfigConstant.TOPIC);

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        long timeMillis = System.currentTimeMillis();
        if (timeMillis <= timestamp) {
            return null;
        } else {
            timestamp = timeMillis + 1000;
        }

        if (position == null) {
            Map<String, Object> offset = context.offsetStorageReader().offset(getPartition(topic));
            if (offset != null) {
                position = (long) offset.get(POSITION_FIELD);
            } else {
                position = -1l;
            }
        }

        String message = UUID.randomUUID().toString();
        SourceRecord sourceRecord = new SourceRecord(getPartition(topic), Collections.singletonMap(POSITION_FIELD, position), topic, VALUE_SCHEMA, message);

        return Collections.singletonList(sourceRecord);
    }

    @Override
    public void stop() {

    }

    public Map<String, String> getPartition(String key) {
        return Collections.singletonMap(POSITION_FIELD, key);
    }
}
