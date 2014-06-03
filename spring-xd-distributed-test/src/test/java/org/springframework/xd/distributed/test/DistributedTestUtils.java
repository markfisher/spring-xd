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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.oracle.tools.runtime.PropertiesBuilder;
import com.oracle.tools.runtime.console.SystemApplicationConsole;
import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.NativeJavaApplicationBuilder;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplicationSchema;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.hateoas.PagedResources;
import org.springframework.xd.batch.hsqldb.server.HsqlServerApplication;
import org.springframework.xd.dirt.server.AdminServerApplication;
import org.springframework.xd.dirt.server.ContainerServerApplication;
import org.springframework.xd.rest.client.domain.ContainerAttributesResource;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;

/**
 * Collection of utilities for starting external processes required
 * for a distributed XD system.
 *
 * @author Patrick Peralta
 */
public class DistributedTestUtils {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(DistributedTestUtils.class);

	/**
	 * Port for ZooKeeper server.
	 */
	public static final int ZOOKEEPER_PORT = 3181;

	/**
	 * Admin server URL.
	 */
	public static final String ADMIN_URL = "http://localhost:9393";


	/**
	 * Start an instance of ZooKeeper. Upon test completion, the method
	 * {@link org.apache.curator.test.TestingServer#stop()} should be invoked
	 * to shut down the server.
	 *
	 * @return ZooKeeper testing server
	 * @throws Exception
	 */
	public static TestingServer startZooKeeper() throws Exception {
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

	/**
	 * Start an instance of the admin server. This method will block until
	 * the admin server is capable of processing HTTP requests. Upon test
	 * completion, the method {@link com.oracle.tools.runtime.java.JavaApplication#close()}
	 * should be invoked to shut down the server.
	 *
	 * @return admin server application reference
	 * @throws IOException            if an exception is thrown launching the process
	 * @throws InterruptedException   if the executing thread is interrupted
	 */
	public static JavaApplication<SimpleJavaApplication> startAdmin() throws IOException, InterruptedException {
		JavaApplication<SimpleJavaApplication> adminServer;

		Properties systemProperties = new Properties();
		systemProperties.setProperty("zk.client.connect", "localhost:" + ZOOKEEPER_PORT);

		adminServer = launch(AdminServerApplication.class, false, systemProperties, null);
		logger.debug("waiting for admin server");
		waitForAdminServer(ADMIN_URL);
		logger.debug("admin server ready");

		return adminServer;
	}

	/**
	 * Start a container instance.  Upon test completion, the method
	 * {@link com.oracle.tools.runtime.java.JavaApplication#close()}
	 * should be invoked to shut down the server. This method may also be
	 * invoked as part of failover testing.
	 * <p />
	 * Note that this method returns immediately. In order to verify
	 * that the container was started, invoke {@link #waitForContainers}
	 * to block until the container(s) are started.
	 *
	 * @return container server application reference
	 * @throws IOException if an exception is thrown launching the process
	 *
	 * @see #waitForContainers
	 */
	public static JavaApplication<SimpleJavaApplication> startContainer() throws IOException {
		Properties systemProperties = new Properties();
		systemProperties.setProperty("zk.client.connect", "localhost:" + ZOOKEEPER_PORT);

		return launch(ContainerServerApplication.class, false, systemProperties, null);
	}

	/**
	 * Start an instance of HSQL. Upon test completion, the method
	 * {@link com.oracle.tools.runtime.java.JavaApplication#close()}
	 * should be invoked to shut down the server.
	 *
	 * @return HSQL server application reference
	 * @throws IOException if an exception is thrown launching the process
	 */
	public static JavaApplication<SimpleJavaApplication> startHsql() throws IOException {
		return launch(HsqlServerApplication.class, false, null, null);
	}

	/**
	 * Block the executing thread until all of the indicated process IDs
	 * have been identified in the list of runtime containers as indicated
	 * by the admin server.
	 *
	 * @param template REST template used to communicate with the admin server
	 * @param pids     set of process IDs for the expected containers
	 * @return map of process id to container id
	 * @throws InterruptedException            if the executing thread is interrupted
	 * @throws java.lang.IllegalStateException if the number of containers identified
	 *         does not match the number of PIDs provided
	 */
	public static Map<Long, String> waitForContainers(SpringXDTemplate template,
			Set<Long> pids) throws InterruptedException, IllegalStateException {
		int pidCount = pids.size();
		Map<Long, String> mapPidUuid = new HashMap<>();
		long expiry = System.currentTimeMillis() + 30000;

		while (mapPidUuid.size() != pidCount && System.currentTimeMillis() < expiry) {
			Thread.sleep(500);
			mapPidUuid.clear();
			PagedResources<ContainerAttributesResource> containers =
					template.runtimeOperations().listRuntimeContainers();
			for (ContainerAttributesResource container : containers) {
				logger.trace("Container: {}", container);
				long pid = Long.parseLong(container.getAttribute("pid"));
				mapPidUuid.put(pid, container.getAttribute("id"));
			}
		}

		if (mapPidUuid.size() == pidCount && mapPidUuid.keySet().containsAll(pids)) {
			return mapPidUuid;
		}

		Set<Long> missingPids = new HashSet<>(pids);
		missingPids.removeAll(mapPidUuid.keySet());

		Set<Long> unexpectedPids = new HashSet<>(mapPidUuid.keySet());
		unexpectedPids.removeAll(pids);

		StringBuilder builder = new StringBuilder();
		if (!missingPids.isEmpty()) {
			builder.append("Admin server did not find the following container PIDs:")
					.append(missingPids);
		}
		if (!unexpectedPids.isEmpty()) {
			if (builder.length() > 0) {
				builder.append("; ");
			}
			builder.append("Admin server found the following unexpected container PIDs:")
					.append(unexpectedPids);
		}

		throw new IllegalStateException(builder.toString());
	}

	/**
	 * Block the executing thread until the admin server is responding to
	 * HTTP requests.
	 *
	 * @param url URL for admin server
	 * @throws InterruptedException            if the executing thread is interrupted
	 * @throws java.lang.IllegalStateException if a successful connection to the
	 *         admin server was not established
	 */
	private static void waitForAdminServer(String url) throws InterruptedException, IllegalStateException {
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

	/**
	 * Launch the given class's {@code main} method in a separate JVM.
	 *
	 * @param clz               class to launch
	 * @param remoteDebug       if true, enable remote debugging
	 * @param systemProperties  system properties for new process
	 * @param args              command line arguments
	 * @return launched application
	 *
	 * @throws IOException if an exception was thrown launching the process
	 */
	private static JavaApplication<SimpleJavaApplication> launch(Class<?> clz,
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

}
