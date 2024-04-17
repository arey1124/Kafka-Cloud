package com.example.kafka.kafkademo;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import net.datafaker.Faker;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

@SpringBootApplication
public class KafkaDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

	@Bean
	NewTopic hobbit2() {
		return TopicBuilder.name("hobbit2")
			.partitions(12)
			.replicas(3)
			.build();
	}

}


@Component
@RequiredArgsConstructor
class Producer {
    private final KafkaTemplate<String, String> template;
    private Faker faker;

    @EventListener(ApplicationStartedEvent.class)
    public void generate() {
		faker = new Faker();
        final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));
        final Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));

        Flux.zip(interval, quotes)
            .map(objects -> template.send("hobbit", String.valueOf(faker.random().nextInt(42)), objects.getT2()))
            .subscribe(); 
    }
}

@Component
class Consumer {
	
	@KafkaListener(topics = {"hobbit"}, groupId = "spring-boot-kafka")
	public void consume(ConsumerRecord<String, String> record) {
		System.out.println("received quote = " + record.value() + " with key : " + record.key());
	}
}
