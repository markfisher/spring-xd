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

package org.springframework.xd.distributed.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import com.oracle.tools.runtime.PropertiesBuilder;
import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.hateoas.PagedResources;
import org.springframework.xd.dirt.server.AdminServerApplication;
import org.springframework.xd.rest.client.domain.StreamDefinitionResource;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;

/**
 * @author Patrick Peralta
 */
public class HelloWorldTest {
	private static final Logger logger = LoggerFactory.getLogger(HelloWorldTest.class);

	public static final int ZOOKEEPER_PORT = 3181;

	public static final String ADMIN_URL = "http://localhost:9393";

	/**
	 * Launch the given class's {@code main} method in a separate JVM.
	 *
	 * @param clz class to launch
	 * @param args command line arguments
	 *
	 * @return launched application
	 *
	 * @throws IOException
	 */
	protected JavaApplication<SimpleJavaApplication> launch(Class<?> clz,
			boolean remoteDebug, Properties systemProperties, List<String> args) throws IOException {
		String classpath = System.getProperty("java.class.path");
		SimpleJavaApplicationSchema schema = new SimpleJavaApplicationSchema(clz.getName(), classpath);
		if (args != null) {
			for (String arg : args) {
				schema.addArgument(arg);
			}
		}
		if (systemProperties != null) {
			schema.setSystemProperties(new PropertiesBuilder(systemProperties));
		}

		NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema> builder =
				new NativeJavaApplicationBuilder<SimpleJavaApplication, SimpleJavaApplicationSchema>();
		builder.setRemoteDebuggingEnabled(remoteDebug);
		return builder.realize(schema, clz.getName(), new SystemApplicationConsole());
	}

	public TestingServer startZooKeeper() throws Exception {
		return new TestingServer(new InstanceSpec(
				null,             // dataDirectory
				ZOOKEEPER_PORT,   // port
				-1,               // electionPort
				-1,               // quorumPort
				true,             // deleteDataDirectoryOnClose
				-1,               // serverId
				500,              // tickTime
				-1));             // maxClientCnxns
	}

	public JavaApplication<SimpleJavaApplication> startAdmin() throws IOException, InterruptedException {
		JavaApplication<SimpleJavaApplication> adminServer;

		Properties systemProperties = new Properties();
		systemProperties.setProperty("zk.client.connect", "localhost:" + ZOOKEEPER_PORT);

		adminServer = launch(AdminServerApplication.class, false, systemProperties, null);
		logger.debug("waiting for admin server");
		waitForServer(ADMIN_URL);
		logger.debug("admin server ready");

		return adminServer;
	}

	@Test
	public void testServers() throws Exception {
		// TODO: start up HSQL
		TestingServer zooKeeper = startZooKeeper();

		JavaApplication<SimpleJavaApplication> adminServer = null;

		try {
			adminServer = startAdmin();

			SpringXDTemplate template = new SpringXDTemplate(new URI(ADMIN_URL));
			template.streamOperations().createStream("distributed-ticktock", "time|log", false);

			PagedResources<StreamDefinitionResource> list = template.streamOperations().list();
			for (StreamDefinitionResource resource : list) {
				logger.info(resource.toString());
				assertEquals("distributed-ticktock", resource.getName());
			}
		}
		finally {
			if (adminServer != null) {
				adminServer.close();
			}
			zooKeeper.stop();
		}

	}

	private static void waitForServer(String url) throws InterruptedException {
		boolean connected = false;
		Exception exception = null;
		int httpStatus = 0;
		long expiry = System.currentTimeMillis() + 30000;
		HttpClient httpclient = new DefaultHttpClient();

		try {
			do {
				try {
					Thread.sleep(100);

					HttpGet httpGet = new HttpGet(url);
					httpStatus = httpclient.execute(httpGet).getStatusLine().getStatusCode();
					if (httpStatus == HttpStatus.SC_OK) {
						connected = true;
					}
				}
				catch (IOException e) {
					exception = e;
				}
			}
			while ((!connected) && System.currentTimeMillis() < expiry);
		}
		finally {
			httpclient.getConnectionManager().shutdown();
		}

		if (!connected) {
			StringBuilder builder = new StringBuilder();
			builder.append("Failed to connect to '").append(url).append("'");
			if (httpStatus > 0) {
				builder.append("; last HTTP status: ").append(httpStatus);
			}
			if (exception != null) {
				StringWriter writer = new StringWriter();
				exception.printStackTrace(new PrintWriter(writer));
				builder.append("; exception: ")
						.append(exception.toString())
						.append(", ").append(writer.toString());
			}
			throw new IllegalStateException(builder.toString());
		}
	}

}
