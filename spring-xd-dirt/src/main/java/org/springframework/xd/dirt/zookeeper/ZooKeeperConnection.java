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

package org.springframework.xd.dirt.zookeeper;

import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

/**
 * A wrapper for a {@link CuratorFramework} instance whose lifecycle is managed as a Spring bean. Accepts
 * {@link ZooKeeperConnectionListener}s to be notified when connection or disconnection events are received.
 * 
 * @author Mark Fisher
 * @author David Turanski
 */
public class ZooKeeperConnection implements SmartLifecycle {

	/**
	 * Logger.
	 */
	private static final Log logger = LogFactory.getLog(ZooKeeperConnection.class);

	/**
	 * The default client connect string. Port 2181 on localhost.
	 */
	public static final String DEFAULT_CLIENT_CONNECT_STRING = "localhost:2181";

	/**
	 * The underlying {@link CuratorFramework} instance.
	 */
	private volatile CuratorFramework curatorFramework;

	/**
	 * Curator client retry policy.
	 */
	private volatile RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

	/**
	 * Connection listener for Curator {@link ConnectionState} events.
	 */
	private final DelegatingConnectionStateListener connectionListener = new DelegatingConnectionStateListener();

	/**
	 * The set of {@link ZooKeeperConnectionListener}s that should be notified for connection and disconnection events.
	 */
	private final CopyOnWriteArraySet<ZooKeeperConnectionListener> listeners = new CopyOnWriteArraySet<ZooKeeperConnectionListener>();

	/**
	 * Flag that indicates whether this connection is currently active within a context.
	 */
	private volatile boolean running;

	/**
	 * Flag that indicates whether this connection should be started automatically.
	 */
	private volatile boolean autoStartup;

	/**
	 * The current ZooKeeper ConnectionState.
	 */
	private volatile ConnectionState currentState;

	private final String clientConnectString;

	/**
	 * Establish a ZooKeeper connection with the default client connect string: {@value #DEFAULT_CLIENT_CONNECT_STRING}
	 */
	public ZooKeeperConnection() {
		this(DEFAULT_CLIENT_CONNECT_STRING);
	}

	/**
	 * Establish a ZooKeeper connection with the provided client connect string.
	 * 
	 * @param clientConnectString one or more {@code host:port} strings, comma-delimited if more than one
	 */
	public ZooKeeperConnection(String clientConnectString) {
		Assert.hasText(clientConnectString, "clientConnectString is required");
		this.clientConnectString = clientConnectString;
	}

	/**
	 * Checks whether the underlying connection is established.
	 * 
	 * @return true if connected
	 */
	public boolean isConnected() {
		return (this.currentState == ConnectionState.CONNECTED || this.currentState == ConnectionState.RECONNECTED);
	}

	/**
	 * Provides access to the underlying {@link CuratorFramework} instance.
	 * 
	 * @return the {@link CuratorFramework} instance
	 */
	public CuratorFramework getClient() {
		return this.curatorFramework;
	}

	/**
	 * Add a {@link ZooKeeperConnectionListener}.
	 * 
	 * @param listener the listener to add
	 * @return true if the listener was added or false if it was already registered
	 */
	public boolean addListener(ZooKeeperConnectionListener listener) {
		return this.listeners.add(listener);
	}

	/**
	 * Remove a {@link ZooKeeperConnectionListener}.
	 * 
	 * @param listener the listener to remove
	 * @return true if the listener was removed or false if it was never registered
	 */
	public boolean removeListener(ZooKeeperConnectionListener listener) {
		return this.listeners.remove(listener);
	}

	// Lifecycle Implementation

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = true;
	}

	@Override
	public int getPhase() {
		// start in the last possible phase
		return Integer.MAX_VALUE;
	}

	/**
	 * Check whether this client is running.
	 */
	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Starts the underlying {@link CuratorFramework} instance.
	 */
	@Override
	public synchronized void start() {
		if (!this.running) {
			this.curatorFramework = CuratorFrameworkFactory.builder()
					// todo: make namespace pluggable so this class can be generic
					.namespace(Paths.XD_NAMESPACE)
					.retryPolicy(this.retryPolicy)
					.connectString(this.clientConnectString)
					.build();
			this.curatorFramework.getConnectionStateListenable().addListener(connectionListener);
			curatorFramework.start();
			this.running = true;
		}
	}

	/**
	 * Closes the underlying {@link CuratorFramework} instance.
	 */
	@Override
	public synchronized void stop() {
		if (this.running) {
			if (this.currentState != null) {
				curatorFramework.close();
			}
			this.running = false;
		}
	}

	/**
	 * Closes the underlying {@link CuratorFramework} instance and then invokes the callback.
	 */
	@Override
	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}


	/**
	 * Listener for Curator {@link ConnectionState} events that delegates to any registered ZooKeeperListeners.
	 */
	private class DelegatingConnectionStateListener implements ConnectionStateListener {

		@Override
		public void stateChanged(CuratorFramework client, ConnectionState newState) {
			currentState = newState;
			switch (newState) {
				case CONNECTED:
				case RECONNECTED:
					logger.info(">>> Curator connected event: " + newState);
					for (ZooKeeperConnectionListener listener : listeners) {
						listener.onConnect(client);
					}
					break;
				case LOST:
				case SUSPENDED:
					logger.info(">>> Curator disconnected event: " + newState);
					for (ZooKeeperConnectionListener listener : listeners) {
						listener.onDisconnect(client);
					}
					break;
				case READ_ONLY:
					// todo: ?
			}
		}
	}


	/**
	 * Override the default retry policy
	 * 
	 * @param retryPolicy Curator client {@link RetryPolicy}
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		Assert.notNull(retryPolicy, "retryPolicy cannot be null");
		this.retryPolicy = retryPolicy;
	}

	public RetryPolicy getRetryPolicy() {
		return this.retryPolicy;
	}

}
