package demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutDown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class);
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

        //Get teh current thread

        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            //
            public void run() {
                log.info("Detected shutdown ,lets exit by calling kafkaConsumer.wakeup");
                kafkaConsumer.wakeup();
                try{
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

        });

        //subscribe to consumer topics
       try {
           kafkaConsumer.subscribe(Collections.singleton(topic));

           while (true) {
               log.info("Polling=>>>");
               ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(1000));
               for (ConsumerRecord<String, String> record : consumerRecord) {
                   log.info("Key->" + record.key() + "\n" +
                           "Value->" + record.value() + "\n" +
                           "Partition->" + record.partition() + "\n" +
                           "Offset->" + record.offset() + "\n"
                   );

               }

           }
       } catch (WakeupException e) {
           log.info("Wakeup Exception");
       } catch (Exception e) {
           log.error("Other exceptions");
       } finally {
           kafkaConsumer.close(); // will commit the offset
       }

    }
}
