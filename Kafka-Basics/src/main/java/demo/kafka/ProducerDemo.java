package demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {

        log.info("kafka Producer =>>>>");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Kafka producer

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        //producer record

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","first java message");

        //send the record to kafka producer

        kafkaProducer.send(producerRecord);

        //flush the producer

        kafkaProducer.flush();

        //close

        kafkaProducer.close();

    }
}
