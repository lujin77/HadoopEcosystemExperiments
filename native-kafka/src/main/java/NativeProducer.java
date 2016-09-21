
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * NativeProducer
 *
 * @author lujin
 * @date 16/9/19
 */
public class NativeProducer {

    public static final String BROKER = "10.0.11.91:9092";
    public static final String TOPIC = "test";

    public static void main(String[] str) throws InterruptedException, IOException {

        System.out.println("Starting ProducerExample ...");

        sendMessages();

    }

    private static void sendMessages() throws InterruptedException, IOException {

        Producer<String, String> producer = createProducer();

        sendMessages(producer);

        // Allow the producer to complete the sending of the records before existing the program.
        Thread.sleep(1000);

    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER);
        props.put("acks", "all");
        props.put("retries", 0);
        // This property controls how much bytes the sender would wait to batch up the content before publishing to Kafka.
        props.put("batch.size", 10);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        return new KafkaProducer(props);
    }

    private static void sendMessages(Producer<String, String> producer) throws InterruptedException {
        int partition = 0;
        long record = 1;
        for (int i = 1; i <= 10; i++) {
            Thread.sleep(1000);
            String key = "key=" + Long.toString(record++);
            String msg = "content=" + String.valueOf(Math.random());
            producer.send(new ProducerRecord<String, String>(TOPIC, partition, key, msg));
            System.out.println("[TRACE] -> " + key + " " + msg);
        }
    }
}
