package demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);
    public static void main(String[] args) {

        log.info("kafka Consumer =>>>>");

        String bootStrapServer = "localhost:9092";
        String groupId="my-second-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Kafka Consumer

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        //subscribe to consumer topics

        kafkaConsumer.subscribe(Collections.singleton(topic));

        while(true){
            log.info("Polling=>>>");
            ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record : consumerRecord){
                log.info("Key->"+record.key() + "\n"+
                                "Value->"+record.value() + "\n"+
                                "Partition->"+record.partition() + "\n"+
                                "Offset->"+record.offset() + "\n"
                        );

            }

        }

    }
}
