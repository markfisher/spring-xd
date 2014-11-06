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

package org.springframework.xd.module.spark;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.core.ResourceConfiguredModule;
import org.springframework.xd.module.options.ModuleOptions;
import org.springframework.xd.spark.SparkModule;


/**
 *
 * @author Ilayaperumal Gopinathan
 */
public class SparkDriver extends ResourceConfiguredModule {

	protected static final String MODULE_INPUT_CHANNEL = "input";

	protected static final String MODULE_OUTPUT_CHANNEL = "output";

	/**
	 * @param descriptor
	 * @param deploymentProperties
	 */
	public SparkDriver(ModuleDescriptor descriptor, ModuleDeploymentProperties deploymentProperties,
			ClassLoader classLoader, ModuleOptions moduleOptions) {
		super(descriptor, deploymentProperties, classLoader, moduleOptions);
	}

//	public static Resource loadSparkConfigurationFile(ClassLoader moduleClassLoader) {
//		Resource sparkXML = new ClassPathResource("/META-INF/spring-xd/spark/spark.xml", moduleClassLoader);
//		return (sparkXML.exists() && sparkXML.isReadable()) ? sparkXML : null;
//	}
//
//	@Override
//	protected void configureModuleApplicationContext(SimpleModuleDefinition moduleDefinition) {
//		super.configureModuleApplicationContext(moduleDefinition);
//		Resource sparkSource = loadSparkConfigurationFile(getClassLoader());
//		if (sparkSource != null) {
//			addSource(sparkSource);
//		}
//	}

	@SuppressWarnings("unchecked")
	@Override
	public void start() {
		super.start();
		new Thread() {
			@Override
			public void run() {
				URLClassLoader classLoader = (URLClassLoader) getClassLoader();
				List<String> jars = new ArrayList<String>();
				for (URL url : classLoader.getURLs()) {
					String file = url.getFile().split("\\!", 2)[0];
					if (file.endsWith(".jar")) {
						jars.add(file);
					}
				}
				URLClassLoader parentClassLoader = (URLClassLoader) getClassLoader().getParent();
				for (URL url : parentClassLoader.getURLs()) {
					String file = url.getFile().split("\\!", 2)[0];
					if (file.endsWith(".jar") && file.contains("xd")) {
						jars.add(file);
					}
				}
				SparkConf conf = new SparkConf().setMaster("spark://igopinathan.local:7077")
						.setAppName("SparkXDModule")
						.setJars(jars.toArray(new String[jars.size()]));
				JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(50000));
//				MessageBusReceiver receiver = getComponent(MessageBusReceiver.class);
				//TODO: support multiple receivers with specific partitions
				Receiver streamingReceiver = getComponent(Receiver.class);
				JavaDStream<String> input = ssc.receiverStream(streamingReceiver);
				SparkModule<String> module = getComponent(SparkModule.class);
				module.execute(input);
				ssc.start();
				ssc.awaitTermination();
			}
		}.start();
	}
}
