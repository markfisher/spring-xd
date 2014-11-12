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

import org.springframework.core.env.Environment;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.core.ResourceConfiguredModule;
import org.springframework.xd.module.options.ModuleOptions;


/**
 *
 * @author Ilayaperumal Gopinathan
 */
public class SparkDriver extends ResourceConfiguredModule {

	private static final String SPARK_MASTER_URL = "spark://localhost:7077";

	private static final String SPARK_STREAMING_BATCH_INTERVAL = "5000";

	private JavaStreamingContext javaStreamingContext;

	/**
	 * @param descriptor
	 * @param deploymentProperties
	 */
	public SparkDriver(ModuleDescriptor descriptor, ModuleDeploymentProperties deploymentProperties,
			ClassLoader classLoader, ModuleOptions moduleOptions) {
		super(descriptor, deploymentProperties, classLoader, moduleOptions);
	}

	@Override
	public void initialize() {
		super.initialize();
		URLClassLoader classLoader = (URLClassLoader) this.getClassLoader();
		List<String> jars = new ArrayList<String>();
		for (URL url : classLoader.getURLs()) {
			String file = url.getFile().split("\\!", 2)[0];
			if (file.endsWith(".jar")) {
				jars.add(file);
			}
		}
		URLClassLoader parentClassLoader = (URLClassLoader) this.getClassLoader().getParent();
		for (URL url : parentClassLoader.getURLs()) {
			String file = url.getFile().split("\\!", 2)[0];
			//TODO: filter out unnecessary jar files
			if (file.endsWith(".jar")) {
				jars.add(file);
			}
		}
		Environment env = this.getApplicationContext().getEnvironment();

		SparkConf sparkConf = new SparkConf().setMaster(env.getProperty("spark.master.url", SPARK_MASTER_URL))
				.setAppName(getDescriptor().getGroup() + "-" + getDescriptor().getModuleLabel())
				.setJars(jars.toArray(new String[jars.size()]));
		this.javaStreamingContext = new JavaStreamingContext(sparkConf, new Duration(Long.valueOf(
				env.getProperty("spark.streaming.batchInterval", SPARK_STREAMING_BATCH_INTERVAL))));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void start() {
		super.start();
		new Thread() {
			@Override
			public void run() {
				//TODO: support multiple receivers with specific partitions
				Receiver streamingReceiver = getComponent(Receiver.class);
				JavaDStream<String> input = javaStreamingContext.receiverStream(streamingReceiver);
				SparkModule<String> module = getComponent(SparkModule.class);
				module.execute(input);
				javaStreamingContext.start();
				javaStreamingContext.awaitTermination();
			}
		}.start();
	}
}
