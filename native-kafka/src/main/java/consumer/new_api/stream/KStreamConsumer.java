package consumer.new_api.stream;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * KStreamConsumer co-ordinate with Storm's topology
 *
 * @author lujin
 * @date 16/9/21
 */
public class KStreamConsumer {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.11.91:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, StringDeserializer.class);
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();
        builder.addSink("kstream", "test");
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

}
