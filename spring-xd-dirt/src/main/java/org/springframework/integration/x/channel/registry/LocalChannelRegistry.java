/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.integration.x.channel.registry;

import java.util.Collection;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.config.ConsumerEndpointFactoryBean;
import org.springframework.integration.core.PollableChannel;
import org.springframework.integration.handler.BridgeHandler;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.util.Assert;

/**
 * A simple implementation of {@link ChannelRegistry} for in-process use. For inbound and outbound, creates a
 * {@link DirectChannel} or a {@link QueueChannel} depending on whether the binding is aliased or not then bridges the
 * passed {@link MessageChannel} to the channel which is registered in the given application context. If that channel
 * does not yet exist, it will be created.
 * 
 * @author David Turanski
 * @author Mark Fisher
 * @author Gary Russell
 * @author Jennifer Hickey
 * @since 1.0
 */
public class LocalChannelRegistry extends ChannelRegistrySupport implements ApplicationContextAware, InitializingBean {

	private volatile AbstractApplicationContext applicationContext;

	private volatile boolean convertWithinTransport = true;

	private int queueSize = Integer.MAX_VALUE;

	private PollerMetadata poller;

	/**
	 * Used in the canonical case, when the binding does not involve an alias name.
	 */
	private SharedChannelProvider<DirectChannel> directChannelProvider = new SharedChannelProvider<DirectChannel>(
			DirectChannel.class) {

		@Override
		protected DirectChannel createSharedChannel(String name) {
			return new DirectChannel();
		}
	};

	/**
	 * Used to create and customize {@link QueueChannel}s when the binding operation involves aliased names.
	 */
	private SharedChannelProvider<QueueChannel> queueChannelProvider = new SharedChannelProvider<QueueChannel>(
			QueueChannel.class) {

		@Override
		protected QueueChannel createSharedChannel(String name) {
			QueueChannel queueChannel = new QueueChannel(queueSize);
			return queueChannel;
		}
	};

	private SharedChannelProvider<PublishSubscribeChannel> pubsubChannelProvider = new SharedChannelProvider<PublishSubscribeChannel>(
			PublishSubscribeChannel.class) {

		@Override
		protected PublishSubscribeChannel createSharedChannel(String name) {
			return new PublishSubscribeChannel();
		}
	};

	/**
	 * Set the size of the queue when using {@link QueueChannel}s.
	 */
	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	/**
	 * Set the poller to use when QueueChannels are used.
	 */
	public void setPoller(PollerMetadata poller) {
		this.poller = poller;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		Assert.isInstanceOf(AbstractApplicationContext.class, applicationContext);
		this.applicationContext = (AbstractApplicationContext) applicationContext;
	}

	/**
	 * Determines whether any conversion logic is applied within the local transport. When false, objects pass through
	 * without any modification; default true.
	 */
	public void setConvertWithinTransport(boolean convertWithinTransport) {
		this.convertWithinTransport = convertWithinTransport;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(applicationContext, "The 'applicationContext' property cannot be null");
	}

	/**
	 * Looks up or creates a DirectChannel with the given name and creates a bridge from that channel to the provided
	 * channel instance.
	 */
	@Override
	public void createInbound(String name, MessageChannel moduleInputChannel, Collection<MediaType> acceptedMediaTypes,
			boolean aliasHint) {
		SharedChannelProvider channelProvider = aliasHint ? queueChannelProvider
				: directChannelProvider;
		doCreateInbound(name, moduleInputChannel, acceptedMediaTypes, channelProvider);
	}

	@Override
	public void createInboundPubSub(String name, MessageChannel moduleInputChannel,
			Collection<MediaType> acceptedMediaTypes) {
		doCreateInbound(name, moduleInputChannel, acceptedMediaTypes, pubsubChannelProvider);
	}

	private void doCreateInbound(String name, MessageChannel moduleInputChannel,
			Collection<MediaType> acceptedMediaTypes,
			SharedChannelProvider<?> channelProvider) {
		Assert.hasText(name, "a valid name is required to register an inbound channel");
		Assert.notNull(moduleInputChannel, "channel must not be null");
		AbstractMessageChannel registeredChannel = channelProvider.lookupOrCreateSharedChannel(name);
		bridge(registeredChannel, moduleInputChannel, registeredChannel.getComponentName() + ".in.bridge",
				acceptedMediaTypes);
	}

	/**
	 * Looks up or creates a DirectChannel with the given name and creates a bridge to that channel from the provided
	 * channel instance.
	 */
	@Override
	public void createOutbound(String name, MessageChannel moduleOutputChannel, boolean aliasHint) {
		SharedChannelProvider channelProvider = aliasHint ? queueChannelProvider
				: directChannelProvider;
		doCreateOutbound(name, moduleOutputChannel, channelProvider);
	}

	@Override
	public void createOutboundPubSub(String name, MessageChannel moduleOutputChannel) {
		doCreateOutbound(name, moduleOutputChannel, pubsubChannelProvider);
	}

	private void doCreateOutbound(String name, MessageChannel moduleOutputChannel,
			SharedChannelProvider<?> channelProvider) {
		Assert.hasText(name, "a valid name is required to register an outbound channel");
		Assert.notNull(moduleOutputChannel, "channel must not be null");
		AbstractMessageChannel registeredChannel = channelProvider.lookupOrCreateSharedChannel(name);
		bridge(moduleOutputChannel, registeredChannel, registeredChannel.getComponentName() + ".out.bridge");
	}

	@Override
	public void deleteInbound(String name) {
		deleteBridges(name + ".in.bridge");
	}

	@Override
	public void deleteOutbound(String name) {
		deleteBridges(name + ".out.bridge");
	}

	@Override
	public void deleteInboundPubSub(String name, MessageChannel channel) {
		deleteBridge(name + ".in.bridge", channel);
	}

	@Override
	public void deleteOutboundPubSub(String name, MessageChannel channel) {
		deleteBridge(name + ".out.bridge", channel);
	}


	protected <T extends AbstractMessageChannel> T createSharedChannel(String name, Class<T> requiredType) {
		try {
			T channel = requiredType.newInstance();
			channel.setComponentName(name);
			channel.setBeanFactory(applicationContext);
			channel.setBeanName(name);
			channel.afterPropertiesSet();
			applicationContext.getBeanFactory().registerSingleton(name, channel);
			return channel;
		}
		catch (Exception e) {
			throw new IllegalArgumentException("failed to create channel: " + name, e);
		}
	}

	protected BridgeHandler bridge(MessageChannel from, MessageChannel to, String bridgeName) {
		return bridge(from, to, bridgeName, null);
	}


	protected BridgeHandler bridge(MessageChannel from, MessageChannel to, String bridgeName,
			final Collection<MediaType> acceptedMediaTypes) {

		final boolean isInbound = bridgeName.endsWith("in.bridge");

		BridgeHandler handler = new BridgeHandler() {

			@Override
			protected Object handleRequestMessage(Message<?> requestMessage) {
				/*
				 * optimization for local transport, just pass through if false
				 */
				if (convertWithinTransport) {
					if (acceptedMediaTypes != null) {
						if (isInbound) {
							return transformInboundIfNecessary(requestMessage, acceptedMediaTypes);
						}
					}
				}
				return requestMessage;
			}

		};

		handler.setOutputChannel(to);
		handler.setBeanName(bridgeName);
		handler.afterPropertiesSet();

		// Usage of a CEFB allows to handle both Subscribable & Pollable channels the same way
		ConsumerEndpointFactoryBean cefb = new ConsumerEndpointFactoryBean();
		cefb.setInputChannel(from);
		cefb.setHandler(handler);
		cefb.setBeanFactory(applicationContext.getBeanFactory());
		if (from instanceof PollableChannel) {
			cefb.setPollerMetadata(poller);
		}
		try {
			cefb.afterPropertiesSet();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		if (!(to instanceof NullChannel)) {
			try {
				cefb.getObject().setComponentName(handler.getComponentName());
				Bridge bridge = isInbound ? new Bridge(to, cefb.getObject())
						: new Bridge(
								from, cefb.getObject());
				addBridge(bridge);
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		cefb.start();
		return handler;
	}

	protected <T> T getBean(String name, Class<T> requiredType) {
		return this.applicationContext.getBean(name, requiredType);
	}

	/**
	 * Looks up or optionally creates a new channel to use.
	 * 
	 * @author Eric Bottard
	 */
	private abstract class SharedChannelProvider<T extends AbstractMessageChannel> {

		private final Class<T> requiredType;

		private SharedChannelProvider(Class<T> clazz) {
			this.requiredType = clazz;
		}

		private final T lookupOrCreateSharedChannel(String name) {
			T channel = lookupSharedChannel(name);
			if (channel == null) {
				channel = createSharedChannel(name);
				channel.setComponentName(name);
				channel.setBeanFactory(applicationContext);
				channel.setBeanName(name);
				channel.afterPropertiesSet();
				applicationContext.getBeanFactory().registerSingleton(name, channel);
			}
			return channel;
		}

		protected abstract T createSharedChannel(String name);

		protected T lookupSharedChannel(String name) {
			T channel = null;
			if (applicationContext.containsBean(name)) {
				try {
					channel = applicationContext.getBean(name, requiredType);
				}
				catch (Exception e) {
					throw new IllegalArgumentException("bean '" + name
							+ "' is already registered but does not match the required type");
				}
			}
			return channel;
		}
	}
}
