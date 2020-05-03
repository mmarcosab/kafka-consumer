package com.service;

import br.com.consumer.deserializer.GsonDeserializer;
import br.com.consumer.service.ConsumerFunction;
import com.ConsumerPessoa;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class PersonService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;

    private final ConsumerFunction parse;

    public PersonService(String groupId, String topic, ConsumerFunction parse, Class<T> type) {
        this(parse, groupId, type);
        consumer.subscribe(Collections.singletonList("TOPICO_KAFKA"));
    }

    public PersonService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type) {
        this(parse, groupId, type);
        consumer.subscribe(Collections.singletonList("TOPICO_KAFKA"));
    }

    private  PersonService(ConsumerFunction parse, String groupId, Class<T> type) {
        this.parse = parse;
        this.consumer =new KafkaConsumer<>(properties(type, groupId));
    }
    public void run(){
        while(true)
        {
            var records = consumer.poll(Duration.ofMillis(100)); //o poll é o instante aonde acontece o commit
            if (!records.isEmpty()) {
                System.out.println("ENCONTREI " + records.count() + " mensagens");
                for (var record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    private Properties properties(Class<T> type, String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, ConsumerPessoa.class.getSimpleName());
//        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // significa que o commit vai acontecer de 1 em 1
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        return properties;
    }


    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
