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

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.module.spark.MessageBusReceiver;

/**
 * @author Mark Fisher
 * @author Ilayaperumal Gopinathan
 */

public class StreamingMessageBusReceiver extends MessageBusReceiver {

	private static final long serialVersionUID = 1L;

	private Channel channel = new Channel();

	private MessageBus messageBus;

	private String channelName;

	public StreamingMessageBusReceiver() {
		super(StorageLevel.MEMORY_ONLY_SER()); // hard-coded for now
	}

	@Override
	public void setInputChannelName(String channelName) {
		this.channelName = channelName;
	}

	public DirectChannel getMessageChannel() {
		return this.channel;
	}

	@Override
	public void onStart() {
		ApplicationContext context = new ClassPathXmlApplicationContext("META-INF/spring-xd/spark/receiver.xml");
		messageBus = context.getBean(MessageBus.class);
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