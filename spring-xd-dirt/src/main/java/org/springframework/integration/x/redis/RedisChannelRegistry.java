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

package org.springframework.integration.x.redis;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.http.MediaType;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.MessageHeaders;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageHandler;
import org.springframework.integration.core.SubscribableChannel;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.x.channel.registry.Bridge;
import org.springframework.integration.x.channel.registry.ChannelRegistry;
import org.springframework.integration.x.channel.registry.ChannelRegistrySupport;
import org.springframework.util.Assert;

/**
 * A {@link ChannelRegistry} implementation backed by Redis.
 * 
 * @author Mark Fisher
 * @author Gary Russell
 * @author David Turanski
 * @author Jennifer Hickey
 */
public class RedisChannelRegistry extends ChannelRegistrySupport implements DisposableBean {

	private RedisConnectionFactory connectionFactory;

	private final EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new EmbeddedHeadersMessageConverter();

	public RedisChannelRegistry(RedisConnectionFactory connectionFactory) {
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		this.connectionFactory = connectionFactory;
	}

	@Override
	public void createInbound(final String name, MessageChannel moduleInputChannel,
			final Collection<MediaType> acceptedMediaTypes, boolean aliasHint) {
		RedisQueueInboundChannelAdapter adapter = new RedisQueueInboundChannelAdapter("queue." + name,
				this.connectionFactory);
		adapter.setEnableDefaultSerializer(false);
		createInbound(name, moduleInputChannel, acceptedMediaTypes, adapter);
	}

	@Override
	public void createInboundPubSub(final String name, MessageChannel moduleInputChannel,
			final Collection<MediaType> acceptedMediaTypes) {
		RedisInboundChannelAdapter adapter = new RedisInboundChannelAdapter(this.connectionFactory);
		adapter.setSerializer(null);
		adapter.setTopics("topic." + name);
		createInbound(name, moduleInputChannel, acceptedMediaTypes, adapter);
	}

	private void createInbound(String name, MessageChannel moduleInputChannel,
			final Collection<MediaType> acceptedMediaTypes, MessageProducerSupport adapter) {
		DirectChannel bridgeToModuleChannel = new DirectChannel();
		bridgeToModuleChannel.setBeanName(name + ".bridge");
		adapter.setOutputChannel(bridgeToModuleChannel);
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
		Assert.isInstanceOf(SubscribableChannel.class, moduleOutputChannel);
		RedisQueueOutboundChannelAdapter queue = new RedisQueueOutboundChannelAdapter("queue." + name,
				connectionFactory);
		queue.setEnableDefaultSerializer(false);
		queue.afterPropertiesSet();
		createOutbound(name, moduleOutputChannel, queue);
	}

	@Override
	public void createOutboundPubSub(final String name, MessageChannel moduleOutputChannel) {
		RedisPublishingMessageHandler topic = new RedisPublishingMessageHandler(connectionFactory);
		topic.setDefaultTopic("topic." + name);
		topic.setSerializer(null);
		topic.afterPropertiesSet();
		createOutbound(name, moduleOutputChannel, topic);
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

		private MessageHandler delegate;


		private SendingHandler(MessageHandler delegate) {
			this.delegate = delegate;
		}

		@Override
		protected void handleMessageInternal(Message<?> message) throws Exception {
			@SuppressWarnings("unchecked")
			Message<byte[]> transformed = (Message<byte[]>) transformOutboundIfNecessary(message,
					MediaType.APPLICATION_OCTET_STREAM);
			Message<?> messageToSend = embeddedHeadersMessageConverter.embedHeaders(transformed,
					MessageHeaders.CONTENT_TYPE, ORIGINAL_CONTENT_TYPE_HEADER);
			Assert.isInstanceOf(byte[].class, messageToSend.getPayload());
			delegate.handleMessage(messageToSend);
		}

	}

	private class ReceivingHandler extends AbstractReplyProducingMessageHandler {

		private final Collection<MediaType> acceptedMediaTypes;

		public ReceivingHandler(Collection<MediaType> acceptedMediaTypes) {
			this.acceptedMediaTypes = acceptedMediaTypes;
		}

		@SuppressWarnings("unchecked")
		@Override
		protected Object handleRequestMessage(Message<?> requestMessage) {
			Message<?> theRequestMessage = requestMessage;
			try {
				theRequestMessage = embeddedHeadersMessageConverter.extractHeaders((Message<byte[]>) requestMessage);
			}
			catch (UnsupportedEncodingException e) {
				logger.error("Could not convert message", e);
			}
			return transformInboundIfNecessary(theRequestMessage, acceptedMediaTypes);
		}

	};

	static class EmbeddedHeadersMessageConverter {

		/**
		 * Encodes requested headers into payload; max headers = 255; max header name length = 255; max header value
		 * length = 255.
		 * 
		 * @throws UnsupportedEncodingException
		 */
		Message<byte[]> embedHeaders(Message<byte[]> message, String... headers) throws UnsupportedEncodingException {
			String[] headerValues = new String[headers.length];
			int n = 0;
			int headerCount = 0;
			int headersLength = 0;
			for (String header : headers) {
				String value = (String) message.getHeaders().get(header);
				headerValues[n++] = value;
				if (value != null) {
					headerCount++;
					headersLength += header.length() + value.length();
				}
			}
			byte[] newPayload = new byte[message.getPayload().length + headersLength + headerCount * 2 + 1];
			ByteBuffer byteBuffer = ByteBuffer.wrap(newPayload);
			byteBuffer.put((byte) headerCount);
			for (int i = 0; i < headers.length; i++) {
				if (headerValues[i] != null) {
					byteBuffer.put((byte) headers[i].length());
					byteBuffer.put(headers[i].getBytes("UTF-8"));
					byteBuffer.put((byte) headerValues[i].length());
					byteBuffer.put(headerValues[i].getBytes("UTF-8"));
				}
			}
			byteBuffer.put(message.getPayload());
			return MessageBuilder.withPayload(newPayload).copyHeaders(message.getHeaders()).build();
		}

		Message<byte[]> extractHeaders(Message<byte[]> message) throws UnsupportedEncodingException {
			byte[] bytes = message.getPayload();
			ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
			int headerCount = byteBuffer.get();
			Map<String, String> headers = new HashMap<String, String>();
			for (int i = 0; i < headerCount; i++) {
				int len = byteBuffer.get();
				String headerName = new String(bytes, byteBuffer.position(), len, "UTF-8");
				byteBuffer.position(byteBuffer.position() + len);
				len = byteBuffer.get();
				String headerValue = new String(bytes, byteBuffer.position(), len, "UTF-8");
				byteBuffer.position(byteBuffer.position() + len);
				headers.put(headerName, headerValue);
			}
			byte[] newPayload = new byte[byteBuffer.remaining()];
			byteBuffer.get(newPayload);
			return MessageBuilder.withPayload(newPayload).copyHeaders(headers).build();
		}

	}
}
