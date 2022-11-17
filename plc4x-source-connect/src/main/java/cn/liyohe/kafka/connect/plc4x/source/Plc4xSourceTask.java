package cn.liyohe.kafka.connect.plc4x.source;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

/**
 * plc4x源任务
 *
 * @author liyohe
 * @date 2022/11/16
 */
public class Plc4xSourceTask extends SourceTask {
    @Override
    public String version() {
        return new Plc4xSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> configs) {

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return null;
    }

    @Override
    public void stop() {

    }
}
