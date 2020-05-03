package br.com.consumer;

import br.com.consumer.entity.Email;

import br.com.consumer.service.EmailService;
import com.entity.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) throws IOException {
        var logService = new LogService();
        try (var service = new EmailService(LogService.class.getSimpleName(),
                Pattern.compile("TOPICO.*"),
                logService::parse,
                Email.class)) {
            service.run();
        }
        try (var service = new com.service.PersonService<>(LogService.class.getSimpleName(),
                Pattern.compile("TOPICO.*"),
                logService::parse,
                Person.class)) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("---------------------------------------------");
        System.out.println("Log de mensagens");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }


}
