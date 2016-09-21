
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


public class NativeConsumer {

    public static final String BROKER = "10.0.11.91:9092";
    public static final String TOPIC = "test";
    public static final String GROUP = "j-group";

    public static void main(String[] str) throws InterruptedException {

        System.out.println("Starting AutoOffsetGuranteedAtLeastOnceConsumer ...");

        execute();

    }


    private static void execute() throws InterruptedException {

        KafkaConsumer<String, String> consumer = createConsumer();

        consumer.subscribe(Arrays.asList(TOPIC));

        processRecords(consumer);

    }

    private static KafkaConsumer<String, String> createConsumer() {

        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER);
        props.put("group.id", GROUP);
//        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "999999999999");
        props.put("max.partition.fetch.bytes", String.valueOf(1024 * 1024));
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "6001");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<String, String>(props);
    }


    private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException {

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(1000);
            long lastOffset = 0;

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                lastOffset = record.offset();
            }

//            System.out.println("lastOffset read: " + lastOffset);

            process();

            // Below call is important to control the offset commit. Do this call after you
            // finish processing the business process to get the at least once guarantee.

            consumer.commitSync();


        }
    }

    private static void process() throws InterruptedException {

        // create some delay to simulate processing of the record.
        Thread.sleep(500);
    }
}

