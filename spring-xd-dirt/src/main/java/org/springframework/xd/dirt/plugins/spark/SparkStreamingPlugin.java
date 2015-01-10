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

package org.springframework.xd.dirt.plugins.spark;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.integration.bus.BusProperties;
import org.springframework.xd.dirt.integration.bus.ConnectionProperties;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.integration.bus.MessageBusSupport;
import org.springframework.xd.dirt.module.spark.LocalMessageBusHolder;
import org.springframework.xd.dirt.module.spark.MessageBusReceiver;
import org.springframework.xd.dirt.module.spark.MessageBusSender;
import org.springframework.xd.dirt.plugins.stream.StreamPlugin;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.core.Module;
import org.springframework.xd.module.core.ModuleFactory;
import org.springframework.xd.module.spark.SparkModule;

/**
 * Plugin for Spark Streaming support.
 *
 * @author Ilayaperumal Gopinathan
 */
public class SparkStreamingPlugin extends StreamPlugin {

//	private JavaSparkContext sparkContext;

	@Autowired
	public SparkStreamingPlugin(MessageBus messageBus, ZooKeeperConnection zkConnection) {
		super(messageBus, zkConnection);
	}

	@Override
	public boolean supports(Module module) {
		String moduleExecutionFramework = module.getProperties().getProperty(ModuleFactory.MODULE_EXECUTION_FRAMEWORK);
		return (moduleExecutionFramework != null &&
				(moduleExecutionFramework.equals(SparkModule.MODULE_EXECUTION_FRAMEWORK)));
	}

	@Override
	public void postProcessModule(Module module) {
		String moduleExecutionFramework = module.getProperties().getProperty(ModuleFactory.MODULE_EXECUTION_FRAMEWORK);
		Assert.notNull(moduleExecutionFramework, "Module execution framework should be set for spark module.");
		ConfigurableApplicationContext moduleContext = module.getApplicationContext();
		String transport = moduleContext.getEnvironment().getProperty("XD_TRANSPORT");
		Properties messageBusProperties = getMessageBusProperties(module, transport);
		Properties inboundModuleProperties = this.extractConsumerProducerProperties(module)[0];
		Properties outboundModuleProperties = this.extractConsumerProducerProperties(module)[1];

		MessageBusReceiver receiver = null;
		if (transport.equals("local")) {
			//todo: if (spark.master.url does not start with "local") throw new IllegalStateException
			LocalMessageBusHolder messageBusHolder = new LocalMessageBusHolder();
			messageBusHolder.set(module.getComponent(MessageBus.class));
			receiver = new MessageBusReceiver(messageBusHolder, messageBusProperties, inboundModuleProperties);
			if (module.getType().equals(ModuleType.processor)) {
				ConfigurableBeanFactory beanFactory = module.getApplicationContext().getBeanFactory();
				MessageBusSender sender = new MessageBusSender(messageBusHolder,
						getOutputChannelName(module), messageBusProperties, outboundModuleProperties);
				beanFactory.registerSingleton("messageBusSender", sender);
			}
		}
		else {
			receiver = new MessageBusReceiver(messageBusProperties, inboundModuleProperties);
			if (module.getType().equals(ModuleType.processor)) {
				ConfigurableBeanFactory beanFactory = module.getApplicationContext().getBeanFactory();
				MessageBusSender sender = new MessageBusSender(getOutputChannelName(module), messageBusProperties, outboundModuleProperties);
				beanFactory.registerSingleton("messageBusSender", sender);
			}
		}

		registerMessageBusReceiver(receiver, module);

	}

	private Properties getMessageBusProperties(Module module, String transport) {
		ConfigurableEnvironment env = module.getApplicationContext().getEnvironment();
		Properties busProperties = new Properties();
		busProperties.put("XD_TRANSPORT", transport);
		if (!transport.equals("local")) {
			List<String> messageBusConnectionProperties = new ArrayList<String>();
			String connectionPropertiesClassName = transport.substring(0, 1).toUpperCase() + transport.substring(1) +
					"ConnectionProperties";
			try {
				Class connectionProperties = Class.forName(ConnectionProperties.PACKAGE_NAME + connectionPropertiesClassName);
				messageBusConnectionProperties.addAll(Arrays.asList(((ConnectionProperties) connectionProperties.newInstance()).getConnectionProperties()));
			}
			catch (ClassNotFoundException cnfe) {
				throw new RuntimeException(String.format("The transport %s must provide class %s", transport, connectionPropertiesClassName));
			}
			catch (ReflectiveOperationException roe) {
				throw new RuntimeException(roe);
			}
			for (String propertyKey : messageBusConnectionProperties) {
				String resolvedValue = env.resolvePlaceholders("${" + propertyKey + "}");
				busProperties.put(propertyKey, resolvedValue);
			}
		}
		List<String> messageBusPropertyKeys = new ArrayList<String>();
		Field[] propertyFields = BusProperties.class.getFields();
		for (Field f : propertyFields) {
			try {
				messageBusPropertyKeys.add((String) f.get("null"));
			}
			catch (IllegalAccessException e) {
				//todo: handle exception
				throw new RuntimeException(e);
			}
		}
		String[] properties = ((MessageBusSupport) messageBus).getMessageBusSpecificProperties();
		messageBusPropertyKeys.addAll(Arrays.asList(properties));
		for (String key : messageBusPropertyKeys) {
			String propertyName = "xd.messagebus." + transport + ".default." + key;
			String resolvedValue = env.resolvePlaceholders("${" + propertyName + "}");
			if (!resolvedValue.contains("${")) {
				busProperties.put(propertyName, resolvedValue);
			}
		}
		return busProperties;
	}

	private void registerMessageBusReceiver(MessageBusReceiver receiver, Module module) {
		receiver.setInputChannelName(getInputChannelName(module));
		ConfigurableBeanFactory beanFactory = module.getApplicationContext().getBeanFactory();
		beanFactory.registerSingleton("messageBusReceiver", receiver);
	}

}
