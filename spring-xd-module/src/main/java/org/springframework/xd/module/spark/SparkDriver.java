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

package org.springframework.xd.module.spark;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.SocketUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.NoOpMessageBusBinderModule;
import org.springframework.xd.module.core.ResourceConfiguredModule;
import org.springframework.xd.module.options.ModuleOptions;

/**
 * The driver that adapts an implementation of {@link SparkModule} to be executed as an XD module.
 *
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 */
public class SparkDriver extends ResourceConfiguredModule implements NoOpMessageBusBinderModule {

	private static final String SPARK_MASTER_URL = "spark://localhost:7077";

	private static final String SPARK_STREAMING_BATCH_INTERVAL = "2000";

	public static final String MESSAGE_BUS_JARS_LOCATION = "file:${XD_HOME}/lib/messagebus/${XD_TRANSPORT}/*.jar";

	public static final String SPARK_MODULE_DEPLOYMENT_ATTRIBUTE = "hasSparkModuleDeployed";

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(SparkDriver.class);

	private PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

	private JavaStreamingContext streamingContext;

	private SparkModuleExecutor executor;

	/**
	 * Create a SparkDriver
	 *
	 * @param descriptor
	 * @param deploymentProperties
	 */
	public SparkDriver(ModuleDescriptor descriptor, ModuleDeploymentProperties deploymentProperties,
			ClassLoader classLoader, ModuleOptions moduleOptions) {
		super(descriptor, deploymentProperties, classLoader, moduleOptions);
	}


	@Override
	public void start() {
		logger.info("starting SparkDriver");
		Environment env = this.getApplicationContext().getEnvironment();
		String[] jars = getApplicationJars();
		String batchInterval = env.getProperty("batchInterval",
				env.getProperty("spark.streaming.batchInterval", SPARK_STREAMING_BATCH_INTERVAL));
		SparkConf sparkConf = new SparkConf().setMaster(env.getProperty("spark.master.url", SPARK_MASTER_URL))
				.setAppName(getDescriptor().getGroup() + "-" + getDescriptor().getModuleLabel())
				.setJars(jars)
				.set("spark.ui.port", String.valueOf(SocketUtils.findAvailableTcpPort()));
		this.streamingContext = new JavaStreamingContext(sparkConf, new Duration(Long.valueOf(batchInterval)));
		super.start();
		//TODO: support multiple receivers with specific partitions
		final Receiver receiver = getComponent(Receiver.class);
		final SparkModule module = getComponent(SparkModule.class);
		final SparkMessageSender sender = (ModuleType.sparkProcessor.equals(getType()))
				? getComponent(SparkMessageSender.class) : null;
		this.executor = new SparkModuleExecutor();
		Executors.newSingleThreadExecutor().execute(new Runnable() {
			@Override
			@SuppressWarnings("unchecked")
			public void run() {
				try {
					JavaDStream input = streamingContext.receiverStream(receiver);
					executor.execute(input, module, sender);
					streamingContext.start();
					streamingContext.awaitTermination();
				}
				catch (Exception e) {
					// ignore
				}
			}
		});
	}

	private String[] getApplicationJars() {
		// Get jars from module classpath
		URLClassLoader classLoader = (URLClassLoader) this.getClassLoader();
		List<String> jars = new ArrayList<String>();
		for (URL url : classLoader.getURLs()) {
			String file = url.getFile().split("\\!", 2)[0];
			if (file.endsWith(".jar")) {
				jars.add(file);
			}
		}
		// Get message bus libraries
		Environment env = this.getApplicationContext().getEnvironment();
		String jarsLocation = env.resolvePlaceholders(MESSAGE_BUS_JARS_LOCATION);
		try {
			Resource[] resources = resolver.getResources(jarsLocation);
			for (Resource resource : resources) {
				URL url = resource.getURL();
				jars.add(url.getFile());
			}
		}
		catch (IOException ioe) {
			// todo:
		}
		// Get necessary dependencies from XD DIRT.
		URLClassLoader parentClassLoader = (URLClassLoader) this.getClassLoader().getParent();
		URL[] urls = parentClassLoader.getURLs();
		for (URL url : urls) {
			String file = url.getFile().split("\\!", 2)[0];
			//if (file.endsWith(".jar")) {
			if (file.endsWith(".jar") && (
					// Add spark jars
					file.contains("spark") ||
							// Add SpringXD dependencies
							file.contains("spring-xd-") ||
							// Add Spring dependencies
							file.contains("spring-core") ||
							file.contains("spring-integration-core") ||
							file.contains("spring-beans") ||
							file.contains("spring-context") ||
							file.contains("spring-boot") ||
							file.contains("spring-aop") ||
							file.contains("spring-expression") ||
							file.contains("spring-messaging") ||
							file.contains("spring-retry") ||
							file.contains("spring-tx") ||
							file.contains("spring-data-commons") ||
							file.contains("spring-data-redis") ||
							file.contains("commons-pool") ||
							file.contains("jedis") ||
							// Add codec dependency
							file.contains("kryo"))) {
				jars.add(file);
			}
		}
		return jars.toArray(new String[jars.size()]);
	}

	@Override
	public void stop() {
		logger.info("stopping SparkDriver");
		try {
			if (executor != null) {
				executor.destroy();
			}
			// todo: when possible (1.3.0), change following two lines to just this one:
			// streamingContext.stop(false, true);
			streamingContext.ssc().sc().cancelAllJobs();
			streamingContext.close();
			super.stop();
		}
		catch (Exception e) {
			// ignore
		}
	}

}
