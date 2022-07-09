package demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
    public static void main(String[] args) {

        log.info("kafka Producer =>>>>");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Kafka producer

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for(int i=0;i<10;i++) {
            //producer record

            ProducerRecord<String, String> producerRecord = new
                    ProducerRecord<>("demo_java", "first java message with callback()" + i);

            //send the record to kafka producer

            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {

                        log.info("Received new message \n" +
                                "Topic->" + metadata.topic() + "\n" +
                                "Partition->" + metadata.partition() + "\n" +
                                "Offset->" + metadata.offset() + "\n" +
                                "TimeStamp->" + metadata.timestamp() + "\n"
                        );
                    } else {
                        log.error("Exception while receiving the data" + exception.getMessage());
                    }
                }
            });
        }
        //flush the producer

        kafkaProducer.flush();

        //close

        kafkaProducer.close();

    }
}
