/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.xd.dirt.module.spark;

import java.io.Serializable;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.redis.RedisAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.xd.dirt.integration.bus.MessageBus;

/**
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */

public class MessageBusReceiver extends Receiver {

	private static final long serialVersionUID = 1L;

	private final String transport;

	private Channel channel;

	private MessageBus messageBus;

	private String channelName;

	public MessageBusReceiver(String transport) {
		super(StorageLevel.MEMORY_ONLY_SER()); // hard-coded for now
		this.transport = transport;
	}

	public void setInputChannelName(String channelName) {
		this.channelName = channelName;
	}

	@Override
	public void onStart() {
		// Set the executor JVM's system property
		System.setProperty("XD_TRANSPORT", transport);
		SpringApplicationBuilder application = new SpringApplicationBuilder()
				.sources(MessageBusConfiguration.class)
				.web(false);
		if (transport.equals("rabbit")) {
			application.sources(RabbitAutoConfiguration.class);
		}
		else if (transport.equals("redis")) {
			application.sources(RedisAutoConfiguration.class);
		}
		application.run();
		channel = new Channel();
		messageBus = application.context().getBean(MessageBus.class);
		messageBus.bindConsumer(channelName, channel, null);
	}

	@Override
	public void onStop() {
		messageBus.unbindConsumers(channelName);
	}

	public class Channel extends DirectChannel implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		protected boolean doSend(Message<?> message, long timeout) {
			store(message.getPayload().toString());
			return true;
		}
	}
}
