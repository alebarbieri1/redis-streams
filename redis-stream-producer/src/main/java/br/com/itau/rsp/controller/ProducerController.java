package br.com.itau.rsp.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.com.itau.rsp.model.Account;
import br.com.itau.rsp.model.DummyEntity;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class ProducerController {

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	private final String STREAM_KEY = "mystream";

	@PostMapping("/produce")
	private ResponseEntity<String> produce() throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		DummyEntity entity = DummyEntity.builder().age(23).message("mock")
				.account(Account.builder().name("my account").number(224252).build()).build();
		String json = mapper.writeValueAsString(entity);
		Map<String, String> hash = new HashMap<>();
		hash.put("payload", json);
		ObjectRecord<String, Object> record = StreamRecords.newRecord().in(STREAM_KEY).ofObject(hash);

		RecordId recordId = redisTemplate.opsForStream().add(record);

		log.info("RecordId => {}", recordId.getValue());

		return ResponseEntity.ok().build();
	}
}