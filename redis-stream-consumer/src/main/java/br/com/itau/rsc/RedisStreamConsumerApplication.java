package br.com.itau.rsc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RedisStreamConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RedisStreamConsumerApplication.class, args);
	}

}
