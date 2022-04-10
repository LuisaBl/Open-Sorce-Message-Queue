import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Properties;

public class Publisher {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        final Logger logger = LoggerFactory.getLogger(Publisher.class);

        //Create properties object for Producer

        Properties prop = new Properties();
        //Set Broker
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //Key/Value Serializer
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //ACKS=ALL
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        //BatchSize
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "4096");

        // Create Producer

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);

        long t1 = System.currentTimeMillis();
        int i = 0;
        for (; i<1000000; ++i){
            //Create ProducerRecord
            ProducerRecord<String, String> record = new ProducerRecord<>("QUEUE", "key1", "value1");

            //Send Data Asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        logger.info("\nReceived record metadata.\n"+
                                "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() +
                                ", Offset: " + recordMetadata.offset() + ", Timestamp: " + recordMetadata.timestamp() + "\n");
                    }else {
                        logger.error("Error Occurred", e);
                    }
                }
            });
        }

        //flush and close Producer
        producer.flush();
        producer.close();
        System.out.println("fertig " + i + " Nachrichten in " + (System.currentTimeMillis() - t1 + " ms"));
    }
}
