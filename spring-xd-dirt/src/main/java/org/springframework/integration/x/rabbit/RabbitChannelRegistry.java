/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.x.rabbit;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.http.MediaType;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageHandler;
import org.springframework.integration.core.SubscribableChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.mapping.AbstractHeaderMapper;
import org.springframework.integration.x.channel.registry.Bridge;
import org.springframework.integration.x.channel.registry.ChannelRegistry;
import org.springframework.integration.x.channel.registry.ChannelRegistrySupport;
import org.springframework.util.Assert;

/**
 * A {@link ChannelRegistry} implementation backed by RabbitMQ.
 * 
 * @author Mark Fisher
 * @author Gary Russell
 * @author Jennifer Hickey
 */
public class RabbitChannelRegistry extends ChannelRegistrySupport implements DisposableBean {

	private final Log logger = LogFactory.getLog(this.getClass());

	private final RabbitAdmin rabbitAdmin;

	private final RabbitTemplate rabbitTemplate = new RabbitTemplate();

	private final ConnectionFactory connectionFactory;

	private volatile Integer concurrentConsumers;

	private final DefaultAmqpHeaderMapper mapper;

	public RabbitChannelRegistry(ConnectionFactory connectionFactory) {
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		this.connectionFactory = connectionFactory;
		this.rabbitTemplate.setConnectionFactory(connectionFactory);
		this.rabbitTemplate.afterPropertiesSet();
		this.rabbitAdmin = new RabbitAdmin(connectionFactory);
		this.rabbitAdmin.afterPropertiesSet();
		this.mapper = new DefaultAmqpHeaderMapper();
		this.mapper.setRequestHeaderNames(new String[] { AbstractHeaderMapper.STANDARD_REQUEST_HEADER_NAME_PATTERN,
			ORIGINAL_CONTENT_TYPE_HEADER });
	}

	@Override
	public void createInbound(final String name, MessageChannel moduleInputChannel,
			final Collection<MediaType> acceptedMediaTypes, boolean aliasHint) {
		if (logger.isInfoEnabled()) {
			logger.info("declaring queue for inbound: " + name);
		}
		Queue queue = new Queue(name);
		this.rabbitAdmin.declareQueue(new Queue(name));
		createInbound(name, moduleInputChannel, acceptedMediaTypes, queue);

	}

	@Override
	public void createInboundPubSub(String name, MessageChannel moduleInputChannel,
			Collection<MediaType> acceptedMediaTypes) {
		Queue queue = this.rabbitAdmin.declareQueue();
		Binding binding = BindingBuilder.bind(queue).to(new FanoutExchange("topic." + name));
		this.rabbitAdmin.declareBinding(binding);
		createInbound(name, moduleInputChannel, acceptedMediaTypes, queue);
	}

	private void createInbound(String name, MessageChannel moduleInputChannel,
			Collection<MediaType> acceptedMediaTypes, Queue queue) {
		SimpleMessageListenerContainer listenerContainer = new SimpleMessageListenerContainer(this.connectionFactory);
		if (this.concurrentConsumers != null) {
			listenerContainer.setConcurrentConsumers(this.concurrentConsumers);
		}
		listenerContainer.setQueues(queue);
		listenerContainer.afterPropertiesSet();
		listenerContainer.afterPropertiesSet();
		AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(listenerContainer);
		DirectChannel bridgeToModuleChannel = new DirectChannel();
		bridgeToModuleChannel.setBeanName(name + ".bridge");
		adapter.setOutputChannel(bridgeToModuleChannel);
		adapter.setHeaderMapper(this.mapper);
		adapter.setBeanName("inbound." + name);
		adapter.afterPropertiesSet();
		addBridge(new Bridge(moduleInputChannel, adapter));
		ReceivingHandler convertingBridge = new ReceivingHandler(acceptedMediaTypes);
		convertingBridge.setOutputChannel(moduleInputChannel);
		convertingBridge.setBeanName(name + ".convert.bridge");
		convertingBridge.afterPropertiesSet();
		bridgeToModuleChannel.subscribe(convertingBridge);
		adapter.start();
	}

	@Override
	public void createOutbound(final String name, MessageChannel moduleOutputChannel, boolean aliasHint) {
		if (logger.isInfoEnabled()) {
			logger.info("declaring queue for outbound: " + name);
		}
		rabbitAdmin.declareQueue(new Queue(name));
		AmqpOutboundEndpoint queue = new AmqpOutboundEndpoint(rabbitTemplate);
		queue.setRoutingKey(name); // uses default exchange
		queue.setHeaderMapper(mapper);
		queue.afterPropertiesSet();
		createOutbound(name, moduleOutputChannel, queue);
	}

	@Override
	public void createOutboundPubSub(String name, MessageChannel moduleOutputChannel) {
		rabbitAdmin.declareExchange(new FanoutExchange("topic." + name));
		AmqpOutboundEndpoint fanout = new AmqpOutboundEndpoint(rabbitTemplate);
		fanout.setExchangeName("topic." + name);
		fanout.setHeaderMapper(mapper);
		fanout.afterPropertiesSet();
		createOutbound(name, moduleOutputChannel, fanout);
	}

	private void createOutbound(final String name, MessageChannel moduleOutputChannel, MessageHandler delegate) {
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		MessageHandler handler = new SendingHandler(delegate);
		EventDrivenConsumer consumer = new EventDrivenConsumer((SubscribableChannel) moduleOutputChannel, handler);
		consumer.setBeanName("outbound." + name);
		consumer.afterPropertiesSet();
		addBridge(new Bridge(moduleOutputChannel, consumer));
		consumer.start();
	}

	@Override
	public void deleteInbound(String name) {
		deleteBridges("inbound." + name);
	}

	@Override
	public void deleteInboundPubSub(String name, MessageChannel inputChannel) {
		deleteBridge("inbound." + name, inputChannel);
	}

	@Override
	public void deleteOutbound(String name) {
		deleteBridges("outbound." + name);
	}

	@Override
	public void deleteOutboundPubSub(String name, MessageChannel outputChannel) {
		deleteBridge("outbound." + name, outputChannel);
	}

	@Override
	public void destroy() {
		stopBridges();
	}

	private class SendingHandler extends AbstractMessageHandler {

		private final MessageHandler delegate;

		private SendingHandler(MessageHandler delegate) {
			this.delegate = delegate;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			// TODO: rabbit wire data pluggable format?
			Message<?> messageToSend = transformOutboundIfNecessary(message, MediaType.APPLICATION_OCTET_STREAM);
			this.delegate.handleMessage(messageToSend);
		}
	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		private final Collection<MediaType> acceptedMediaTypes;

		public ReceivingHandler(Collection<MediaType> acceptedMediaTypes) {
			this.acceptedMediaTypes = acceptedMediaTypes;
		}

		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			return transformInboundIfNecessary(requestMessage, acceptedMediaTypes);
		}

		@Override
		protected boolean shouldCopyRequestHeaders() {
			/*
			 * we've already copied the headers so no need for the ARPMH to do it, and we don't want the content-type
			 * restored if absent.
			 */
			return false;
		}

	};

}
