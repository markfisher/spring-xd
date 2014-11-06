/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.xd.dirt.plugins.stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.module.spark.StreamingMessageBusReceiver;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.core.Module;

/**
 * @author Ilayaperumal Gopinathan
 */
public class SparkPlugin extends StreamPlugin {

	@Autowired
	public SparkPlugin(MessageBus messageBus, ZooKeeperConnection zkConnection) {
		super(messageBus, zkConnection);
	}

	@Override
	public boolean supports(Module module) {
		return (module.getName().contains("spark") && (module.getType().equals(ModuleType.processor) || module.getType().equals(ModuleType.sink)));
	}

	@Override
	public void postProcessModule(Module module) {
		ConfigurableBeanFactory beanFactory = module.getApplicationContext().getBeanFactory();
		StreamingMessageBusReceiver receiver = new StreamingMessageBusReceiver();
		receiver.setInputChannelName(getInputChannelName(module));
		beanFactory.registerSingleton("streamingMessageBusReceiver", receiver);
	}

}
