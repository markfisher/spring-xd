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

package org.springframework.xd.dirt.curator;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import org.springframework.context.SmartLifecycle;

/**
 * ZooKeeper server process that can run embedded within another process. Used by the
 * {@link org.springframework.xd.dirt.server.SingleNodeApplication} if ZooKeeper is not already available.
 * 
 * @author Mark Fisher
 */
public class EmbeddedZooKeeper implements SmartLifecycle {

	/**
	 * Logger.
	 */
	private static final Log logger = LogFactory.getLog(EmbeddedZooKeeper.class);

	private int clientPort = 2181;

	/**
	 * The ZooKeeper server instance.
	 */
	private final ZooKeeperServerMain server = new ZooKeeperServerMain();

	/**
	 * The handle to the submitted server task that enables interruption.
	 */
	private volatile Future<?> task;

	/**
	 * Specify the clientPort. Default is 2181.
	 * 
	 * @param clientPort port clients shoud use to connect
	 */
	public void setClientPort(int clientPort) {
		this.clientPort = clientPort;
	}

	/**
	 * Start the ZooKeeper server.
	 */
	@Override
	public void start() {
		Properties props = new Properties();
		props.setProperty("dataDir", System.getProperty("java.io.tmpdir") + "/data");
		props.setProperty("clientPort", "" + clientPort);
		QuorumPeerConfig quorumConfig = new QuorumPeerConfig();
		try {
			quorumConfig.parseProperties(props);
		}
		catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
		final ServerConfig configuration = new ServerConfig();
		configuration.readFrom(quorumConfig);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		this.task = executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					server.runFromConfig(configuration);
				}
				catch (IOException e) {
					logger.error("failed to start embedded ZooKeeper", e);
				}
			}
		});
	}

	@Override
	public void stop() {
		if (task != null) {
			task.cancel(true);
		}
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isRunning() {
		return (this.task != null && !task.isCancelled() && !task.isDone());
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	public static void main(String[] args) {
		EmbeddedZooKeeper zk = new EmbeddedZooKeeper();
		zk.start();
	}

}
