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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.Properties;

import javax.net.SocketFactory;

import com.oracle.tools.runtime.PropertiesBuilder;
import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
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
	protected JavaApplication<SimpleJavaApplication> launch(Class<?> clz, boolean remoteDebug, Properties systemProperties, String... args) throws IOException {
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

	@Test
	public void testServers() throws Exception {
		TestingServer zooKeeper = new TestingServer(new InstanceSpec(
				null,             // dataDirectory
				ZOOKEEPER_PORT,   // port
				-1,               // electionPort
				-1,               // quorumPort
				true,             // deleteDataDirectoryOnClose
				-1,               // serverId
				500,              // tickTime
				-1));             // maxClientCnxns

		JavaApplication<SimpleJavaApplication> adminServer = null;
		Properties systemProperties = new Properties();
		systemProperties.setProperty("zk.client.connect", "localhost:" + ZOOKEEPER_PORT);

		try {
			adminServer = launch(AdminServerApplication.class, false, systemProperties, null);
			logger.warn("waiting for admin server");
//			waitForServer(9393);
			Thread.sleep(10000);
			logger.warn("admin server ready");

			SpringXDTemplate template = new SpringXDTemplate(new URI("http://localhost:9393"));

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

	/**
	 * Block the executing thread until a socket connection can be established
	 * with the server.
	 *
	 * @param port number for server
	 * @throws InterruptedException
	 */
	private static void waitForServer(int port) throws InterruptedException {
		boolean canConnect = false;
		int tries = 0;
		Socket socket = null;

		do {
			try {
				Thread.sleep(100);
				socket = SocketFactory.getDefault().createSocket();
				socket.connect(new InetSocketAddress("localhost", port), 5000);
				canConnect = true;
				socket.close();
				socket = null;
			}
			catch (IOException e) {
				// ignore
			}
			finally {
				if (socket != null) {
					try {
						socket.close();
					}
					catch (IOException e) {
						// ignore
					}
				}
			}
		}
		while ((!canConnect) && tries < 100);

		if (!canConnect) {
			throw new IllegalStateException("Cannot connect to server on port " + port);
		}
	}
}
