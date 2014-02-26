/*
 * Copyright 2013-2014 the original author or authors.
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

package org.springframework.xd.dirt.container;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;

import javax.net.SocketFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.RollingFileAppender;
import org.apache.zookeeper.CreateMode;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.curator.Paths;
import org.springframework.xd.dirt.server.options.XDPropertyKeys;

/**
 * @author Mark Fisher
 * @author Jennifer Hickey
 * @author David Turanski
 * @author Ilayaperumal Gopinathan
 */
public class XDContainer implements SmartLifecycle {

	private static final String LINE_SEPARATOR = System.getProperty("line.separator");

	private static final Log logger = LogFactory.getLog(XDContainer.class);

	/**
	 * Base location for XD config files. Chosen so as not to collide with user provided content.
	 */
	public static final String XD_CONFIG_ROOT = "META-INF/spring-xd/";

	/**
	 * Where container related config files reside.
	 */
	public static final String XD_INTERNAL_CONFIG_ROOT = XD_CONFIG_ROOT + "internal/";

	private static final String LOG4J_FILE_APPENDER = "file";

	private volatile ConfigurableApplicationContext context;

	private final String id;

	private String hostName;

	private String ipAddress;

	private volatile String jvmName;

	private volatile boolean containerRunning;

	/**
	 * ZooKeeper client connect string.
	 * 
	 * todo: make this pluggable
	 */
	private final String zookeeperClientConnectString = "localhost:2181";

	/**
	 * Curator client.
	 */
	private final CuratorFramework client;

	/**
	 * Curator client retry policy.
	 * 
	 * todo: make pluggable
	 */
	private final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

	/**
	 * Connection listener for Curator client.
	 */
	private final ConnectionListener connectionListener = new ConnectionListener();

	/**
	 * Creates a container with a given id
	 * 
	 * @param id the id
	 */
	public XDContainer(String id) {
		this.id = id;
		client = CuratorFrameworkFactory.builder()
				.namespace(Paths.XD_NAMESPACE)
				.retryPolicy(retryPolicy)
				.connectString(zookeeperClientConnectString).build();
		client.getConnectionStateListenable().addListener(connectionListener);
	}

	/**
	 * Default constructor generates a random id
	 */
	public XDContainer() {
		this(UUID.randomUUID().toString());
	}


	public String getId() {
		return this.id;
	}

	public String getHostName() {
		try {
			this.hostName = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException uhe) {
			this.hostName = "unknown";
		}
		return this.hostName;
	}

	public String getIpAddress() {
		try {
			this.ipAddress = InetAddress.getLocalHost().getHostAddress();
		}
		catch (UnknownHostException uhe) {
			this.ipAddress = "unknown";
		}
		return this.ipAddress;
	}

	public String getJvmName() {
		synchronized (this) {
			if (this.jvmName == null) {
				this.jvmName = ManagementFactory.getRuntimeMXBean().getName();
			}
		}
		return this.jvmName;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public boolean isRunning() {
		return this.context != null;
	}

	private boolean isContainerRunning() {
		return containerRunning;
	}

	public void setContext(ConfigurableApplicationContext context) {
		this.context = context;
	}

	public ApplicationContext getApplicationContext() {
		return this.context;
	}

	/**
	 * This will initiate a ZooKeeper client connection if ZooKeeper appears to be available. Otherwise, since ZooKeeper
	 * is not yet required, it will bypass that connection. In the case that it does establish a connection, the
	 * container host and pid will be written to an ephemeral znode under /xd/containers. The name of the created node
	 * will match this container's id.
	 */
	@Override
	public void start() {
		if (testClientConnectString()) {
			client.start();
		}
		else {
			logger.warn("ZooKeeper does not appear to be running for client connect string: "
					+ zookeeperClientConnectString);
		}
	}

	private void registerWithZooKeeper() {
		try {
			// Paths.ensurePath(client, Paths.DEPLOYMENTS);
			Paths.ensurePath(client, Paths.CONTAINERS);

			// deployments = new PathChildrenCache(client, Paths.DEPLOYMENTS + "/" + this.getId(), false);
			// deployments.getListenable().addListener(deploymentListener);

			String mxBeanName = ManagementFactory.getRuntimeMXBean().getName();
			String tokens[] = mxBeanName.split("@");
			StringBuilder builder = new StringBuilder()
					.append("pid=").append(tokens[0])
					.append(System.lineSeparator())
					.append("host=").append(tokens[1]);

			client.create().withMode(CreateMode.EPHEMERAL).forPath(
					Paths.CONTAINERS + "/" + this.getId(), builder.toString().getBytes("UTF-8"));

			// deployments.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

			logger.info("Started container " + this.getId() + " with attributes: " + builder);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void stop() {
		if (this.context != null && isContainerRunning()) {
			this.containerRunning = false;
			// Publish the container stopped event before the context is closed.
			this.context.publishEvent(new ContainerStoppedEvent(this));
			this.context.close();
			((ConfigurableApplicationContext) context.getParent()).close();
			if (logger.isInfoEnabled()) {
				final String message = "Stopped container: " + this.jvmName;
				final StringBuilder sb = new StringBuilder(LINE_SEPARATOR);
				sb.append(StringUtils.rightPad("", message.length(), "-")).append(LINE_SEPARATOR).append(message).append(
						LINE_SEPARATOR).append(StringUtils.rightPad("", message.length(), "-")).append(LINE_SEPARATOR);
				logger.info(sb.toString());
			}
		}
	}

	@Override
	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}

	public void addListener(ApplicationListener<?> listener) {
		Assert.state(this.context != null, "context is not initialized");
		this.context.addApplicationListener(listener);
	}

	/**
	 * Update container log appender file name with container id
	 */
	private void updateLoggerFilename() {
		Appender appender = Logger.getRootLogger().getAppender(LOG4J_FILE_APPENDER);
		if (appender instanceof RollingFileAppender) {
			String xdHome = context.getEnvironment().getProperty(XDPropertyKeys.XD_HOME);
			((RollingFileAppender) appender).setFile(new File(xdHome).getAbsolutePath()
					+ "/logs/container-" + this.getId() + ".log");
			((RollingFileAppender) appender).activateOptions();
		}
	}

	public int getJmxPort() {
		return Integer.valueOf(this.context.getEnvironment().getProperty(XDPropertyKeys.XD_JMX_PORT));
	}

	public boolean isJmxEnabled() {
		return Boolean.valueOf(this.context.getEnvironment().getProperty(XDPropertyKeys.XD_JMX_ENABLED));
	}

	public String getPropertyValue(String key) {
		return this.context.getEnvironment().getProperty(key);
	}

	private boolean testClientConnectString() {
		for (String address : this.zookeeperClientConnectString.split(",")) {
			String[] hostAndPort = address.trim().split(":");
			try {
				Socket socket = SocketFactory.getDefault().createSocket(
						hostAndPort[0], Integer.parseInt(hostAndPort[1]));
				socket.close();
				return true;
			}
			catch (Exception e) {
				// keep trying, will return false if all fail
			}
		}
		return false;
	}

	private class ConnectionListener implements ConnectionStateListener {

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			switch (newState) {
				case CONNECTED:
				case RECONNECTED:
					logger.info(">>> Curator connected event: " + newState);
					registerWithZooKeeper();
					break;
				case LOST:
				case SUSPENDED:
					logger.info(">>> Curator disconnected event: " + newState);
					break;
				case READ_ONLY:
					// todo: ???
			}
		}
	}

}
