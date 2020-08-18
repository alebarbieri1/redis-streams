package br.com.itau.rsc.consumer;

import java.time.Duration;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandKeyword;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@EnableScheduling
public class StreamConsumer
		implements StreamListener<String, MapRecord<String, Object, String>>, InitializingBean, DisposableBean {

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	private StreamMessageListenerContainer<String, MapRecord<String, Object, String>> listenerContainer;
	private Subscription subscription;
	private final String CONSUMER_NAME = "consumer_1";
	private final String CONSUMER_GROUP_NAME = "mysql";
	private final String STREAM_NAME = "mystream";

	@Override
	public void onMessage(MapRecord<String, Object, String> message) {
		try {
			log.info("MessageId: " + message.getId());
			log.info("Stream: " + message.getStream());
			log.info("Body: " + message.getValue());

			//redisTemplate.opsForStream().acknowledge(CONSUMER_GROUP_NAME, message);
			log.info("Message has been processed");
		} catch (Exception ex) {
			// log the exception and increment the number of errors count
			log.error("Failed to process the message: {} ", message.getId().getValue(), ex);
		}

	}

	@Override
	public void destroy() throws Exception {
		if (subscription != null) {
			subscription.cancel();
		}

		if (listenerContainer != null) {
			listenerContainer.stop();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void afterPropertiesSet() throws Exception {
		try {
			if (!redisTemplate.hasKey(STREAM_NAME)) {
				// Creating consumer group and stream
				log.info("{} does not exist. Creating stream along with the consumer group", STREAM_NAME);
				RedisAsyncCommands commands = (RedisAsyncCommands) redisTemplate.getConnectionFactory().getConnection()
						.getNativeConnection();
				CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).add(CommandKeyword.CREATE)
						.add(STREAM_NAME).add(CONSUMER_GROUP_NAME).add("0").add("MKSTREAM");
				commands.dispatch(CommandType.XGROUP, new StatusOutput<>(StringCodec.UTF8), args);
			} else {
				// Creating consumer group
				redisTemplate.opsForStream().createGroup(STREAM_NAME, ReadOffset.from("0"), CONSUMER_GROUP_NAME);
			}
		} catch (Exception ex) {
			log.info("Consumer group already present: {}", CONSUMER_GROUP_NAME);
		}

		this.listenerContainer = StreamMessageListenerContainer.create(redisTemplate.getConnectionFactory(),
				StreamMessageListenerContainer.StreamMessageListenerContainerOptions.builder()
						.hashKeySerializer(new StringRedisSerializer()).hashValueSerializer(new StringRedisSerializer())
						.pollTimeout(Duration.ofMillis(100)).build());

		this.subscription = listenerContainer.receive(Consumer.from(CONSUMER_GROUP_NAME, CONSUMER_NAME),
				StreamOffset.create(STREAM_NAME, ReadOffset.lastConsumed()), this);

		subscription.await(Duration.ofSeconds(2));
		listenerContainer.start();
	}
}
