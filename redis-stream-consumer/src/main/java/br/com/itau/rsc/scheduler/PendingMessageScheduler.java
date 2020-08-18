package br.com.itau.rsc.scheduler;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableScheduling
@Component
public class PendingMessageScheduler {

	private final String CONSUMER_NAME = "consumer_1";
	private final String CONSUMER_GROUP_NAME = "mysql";
	private final String STREAM_NAME = "mystream";
	private final Integer MAX_NUMBER_FETCH = 10;
	private final Integer MAX_RETRY = 3;

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	@Scheduled(fixedRate = 20000)
	public void processPendingMessage() throws InterruptedException, ExecutionException {
		PendingMessages messages = redisTemplate.opsForStream().pending(STREAM_NAME, CONSUMER_GROUP_NAME,
				Range.unbounded(), MAX_NUMBER_FETCH);
		for (PendingMessage message : messages) {
			// claim message
			boolean claimed = claimMessage(message);
			if (claimed) {
				processMessage(message);
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private boolean claimMessage(PendingMessage pendingMessage) throws InterruptedException, ExecutionException {
		RedisAsyncCommands commands = (RedisAsyncCommands) redisTemplate.getConnectionFactory().getConnection()
				.getNativeConnection();
		// Message will only be claimed if it has been idle by 60 seconds or more
		CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).add(STREAM_NAME).add(CONSUMER_GROUP_NAME)
				.add(CONSUMER_NAME).add("60000").add(pendingMessage.getIdAsString());
		Object output = commands.dispatch(CommandType.XCLAIM, new StatusOutput<>(StringCodec.UTF8), args).get();

		if (output != null) {
			log.info("Message: " + pendingMessage.getIdAsString() + " has been claimed by " + CONSUMER_GROUP_NAME + ":"
					+ CONSUMER_NAME);
			return true;
		}

		return false;
	}

	private void processMessage(PendingMessage pendingMessage) {
		List<MapRecord<String, Object, Object>> messagesToProcess = redisTemplate.opsForStream().range(STREAM_NAME,
				Range.closed(pendingMessage.getIdAsString(), pendingMessage.getIdAsString()));

		if (messagesToProcess == null || messagesToProcess.isEmpty()) {
			log.error("Message is not present. It has been either processed or deleted by some other process : {}",
					pendingMessage.getIdAsString());
		} else if (pendingMessage.getTotalDeliveryCount() <= MAX_RETRY) {
			try {
				MapRecord<String, Object, Object> message = messagesToProcess.get(0);
				log.info("Processing pending message by consumer {}", CONSUMER_NAME);
				log.info("MessageId: " + message.getId());
				log.info("Stream: " + message.getStream());
				log.info("Body: " + message.getValue());
				redisTemplate.opsForStream().acknowledge(CONSUMER_GROUP_NAME, message);
				log.info("Message has been processed after retrying");
			} catch (Exception ex) {
				log.error("Failed to process the message: {} ", messagesToProcess.get(0).getValue(), ex);
			}
		} else {
			// Implement some alert
			log.info("Message {} exceeded maximum retries", pendingMessage.getIdAsString());
		}
	}
}