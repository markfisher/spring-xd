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

import java.util.Properties;

import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.redis.RedisAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.module.spark.SparkMessageSender;

/**
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 */
public class MessageBusSender extends SparkMessageSender {

	private final Properties properties;

	private final String outputChannelName;

	private MessageBus messageBus;

	private boolean started;

	public MessageBusSender(String outputChannelName, Properties properties) {
		this.outputChannelName = outputChannelName;
		this.properties = properties;
	}

	public void start() {
		if (started) {
			return;
		}
		String transport = properties.getProperty("XD_TRANSPORT");
		SpringApplicationBuilder application = new SpringApplicationBuilder()
				.sources(MessageBusConfiguration.class)
						//Make sure the properties are always added at the first precedence level.
				.listeners(new ApplicationListener<ApplicationEnvironmentPreparedEvent>() {
					@Override
					public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
						event.getEnvironment().getPropertySources().addFirst(
								new PropertiesPropertySource("executorEnvironment", properties));
					}
				})
				.web(false);
		if (transport.equals("rabbit")) {
			application.sources(RabbitAutoConfiguration.class);
		}
		else if (transport.equals("redis")) {
			application.sources(RedisAutoConfiguration.class);
		}
		application.run();
		messageBus = application.context().getBean(MessageBus.class);
		messageBus.bindProducer(outputChannelName, this, null);
		started = true;
	}

	public void stop() {
		if (messageBus != null) {
			messageBus.unbindProducer(outputChannelName, this);
		}
		started = false;
	}
}
