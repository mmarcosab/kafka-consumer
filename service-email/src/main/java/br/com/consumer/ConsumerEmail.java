package br.com.consumer;

import br.com.consumer.service.EmailService;
import br.com.consumer.entity.Email;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class ConsumerEmail {

    public static void main(String[] args) throws IOException {
        var consumerTopicoKafka = new ConsumerEmail();
        try(var service = new EmailService(EmailService.class.getSimpleName(), "TOPICO_KAFKA_2", consumerTopicoKafka::parse, Email.class)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------------");
        System.out.println("Enviando email ..  ");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println("Email enviado ..  ");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
