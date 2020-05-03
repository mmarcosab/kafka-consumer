package br.com.consumer;

import br.com.consumer.entity.Person;
import br.com.consumer.service.PersonService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class ConsumerPessoa {

    public static void main(String[] args) throws IOException {
        var consumerTopicoKafka = new ConsumerPessoa();
        try(var service = new PersonService(PersonService.class.getSimpleName(), "TOPICO_KAFKA", consumerTopicoKafka::parse, Person.class)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------------");
        System.out.println("Processando nova mensagem..  ");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println("Processamento feito ..  ");
        try {
            Thread.sleep(5000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
