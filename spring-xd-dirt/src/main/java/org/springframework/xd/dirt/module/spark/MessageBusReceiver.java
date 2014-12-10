/*
 * Copyright 2015 the original author or authors.
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
import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.xd.dirt.integration.bus.MessageBus;

/**
 * Spark {@link Receiver} implementation that binds to the MessageBus as a consumer.
 * 
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */
public class MessageBusReceiver extends Receiver {

	private static final long serialVersionUID = 1L;

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(MessageBusReceiver.class);

	private MessageBus messageBus;

	private ConfigurableApplicationContext applicationContext;

	private String channelName;

	private final Properties messageBusProperties;

	private final Properties moduleConsumerProperties;

	public MessageBusReceiver(Properties messageBusProperties, Properties moduleConsumerProperties) {
		super(StorageLevel.MEMORY_ONLY_SER()); // hard-coded for now
		this.messageBusProperties = messageBusProperties;
		this.moduleConsumerProperties = moduleConsumerProperties;
	}

	public void setInputChannelName(String channelName) {
		this.channelName = channelName;
	}

	@Override
	public void onStart() {
		logger.info("starting MessageBusReceiver");
		applicationContext = MessageBusConfiguration.createApplicationContext(messageBusProperties);
		messageBus = applicationContext.getBean(MessageBus.class);
		messageBus.bindConsumer(channelName, new Channel(), moduleConsumerProperties);
	}

	@Override
	public void onStop() {
		logger.info("stopping MessageBusReceiver");
		messageBus.unbindConsumers(channelName);
		applicationContext.close();
	}


	private class Channel extends DirectChannel implements Serializable {

		private static final long serialVersionUID = 1L;

		@Override
		protected boolean doSend(Message<?> message, long timeout) {
			store(message.getPayload().toString());
			return true;
		}
	}

}
