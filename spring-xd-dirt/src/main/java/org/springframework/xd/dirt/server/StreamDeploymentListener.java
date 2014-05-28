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

package org.springframework.xd.dirt.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.cluster.ContainerMatcher;
import org.springframework.xd.dirt.cluster.ContainerRepository;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.util.DeploymentPropertiesUtility;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.ModuleDeploymentProperties;
import org.springframework.xd.module.ModuleDescriptor;

/**
 * Listener implementation that handles stream deployment requests.
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public class StreamDeploymentListener implements PathChildrenCacheListener {

	/**
	 * Logger.
	 */
	private final Logger logger = LoggerFactory.getLogger(StreamDeploymentListener.class);

	/**
	 * Utility for writing module deployment requests to ZooKeeper.
	 */
	private final ModuleDeploymentWriter moduleDeploymentWriter;

	/**
	 * Utility for loading streams and jobs (including deployment metadata).
	 */
	private final DeploymentLoader deploymentLoader = new DeploymentLoader();

	/**
	 * Stream factory.
	 */
	private final StreamFactory streamFactory;

	/**
	 * Executor service dedicated to handling events raised from
	 * {@link org.apache.curator.framework.recipes.cache.PathChildrenCache}.
	 *
	 * @see #childEvent
	 * @see StreamDeploymentListener.EventHandler
	 */
	private final ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable runnable) {
			Thread thread = new Thread(runnable, "Stream Deployer");
			thread.setDaemon(true);
			return thread;
		}
	});


	/**
	 * Construct a StreamDeploymentListener.
	 *
	 * @param zkConnection ZooKeeper connection
	 * @param containerRepository repository to obtain container data
	 * @param streamFactory factory to construct {@link Stream}
	 * @param containerMatcher matches modules to containers
	 */
	public StreamDeploymentListener(ZooKeeperConnection zkConnection,
			ContainerRepository containerRepository,
			StreamFactory streamFactory,
			ContainerMatcher containerMatcher) {
		this.moduleDeploymentWriter = new ModuleDeploymentWriter(zkConnection,
				containerRepository, containerMatcher);
		this.streamFactory = streamFactory;
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Handle child events for the {@link Paths#STREAMS} path.
	 */
	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		ZooKeeperUtils.logCacheEvent(logger, event);
		executorService.submit(new EventHandler(client, event));
	}

	/**
	 * Handle the creation of a new stream deployment.
	 *
	 * @param client curator client
	 * @param data stream deployment request data
	 */
	private void onChildAdded(CuratorFramework client, ChildData data) throws Exception {
		String streamName = Paths.stripPath(data.getPath());
		Stream stream = deploymentLoader.loadStream(client, streamName, streamFactory);
		if (stream != null) {
			logger.info("Deploying stream {}", stream);
			prepareStream(client, stream);
			deployStream(stream);
			logger.info("Stream {} deployment attempt complete", stream);
		}
	}

	/**
	 * Prepare the new stream for deployment. This updates the ZooKeeper znode for the stream by adding the following
	 * under {@code /xd/streams/[stream-name]}:
	 * <ul>
	 * <li>{@code .../source/[module-name.module-label]}</li>
	 * <li>{@code .../processor/[module-name.module-label]}</li>
	 * <li>{@code .../sink/[module-name.module-label]}</li>
	 * </ul>
	 * The children of these nodes will be ephemeral nodes written by the containers that accept deployment of the
	 * modules.
	 *
	 * @param client curator client
	 * @param stream stream to be prepared
	 */
	private void prepareStream(CuratorFramework client, Stream stream) throws Exception {
		for (Iterator<ModuleDescriptor> iterator = stream.getDeploymentOrderIterator(); iterator.hasNext();) {
			ModuleDescriptor descriptor = iterator.next();
			String streamName = stream.getName();
			String moduleType = descriptor.getModuleDefinition().getType().toString();
			String moduleLabel = descriptor.getModuleLabel();

			String path = new StreamDeploymentsPath()
					.setStreamName(streamName)
					.setModuleType(moduleType)
					.setModuleLabel(moduleLabel).build();

			try {
				client.create().creatingParentsIfNeeded().forPath(path);
			}
			catch (KeeperException.NodeExistsException e) {
				// todo: this would be somewhat unexpected
				logger.info("Path {} already exists", path);
			}
		}
	}

	/**
	 * Issue deployment requests for the modules of the given stream.
	 *
	 * @param stream stream to be deployed
	 *
	 * @throws Exception
	 */
	private void deployStream(final Stream stream) throws Exception {
		final Map<ModuleDescriptor.Key, Integer> mapModuleCount = new HashMap<ModuleDescriptor.Key, Integer>();
		final List<ModuleDescriptor> moduleDescriptors = new ArrayList<ModuleDescriptor>(stream.getDescriptors());
		final Map<ModuleDescriptor.Key, ModuleDeploymentProperties> mapDeploymentProperties =
				new HashMap<ModuleDescriptor.Key, ModuleDeploymentProperties>();

		ModuleDeploymentWriter.ModuleDeploymentPropertiesProvider provider =
				new ModuleDeploymentWriter.ModuleDeploymentPropertiesProvider() {

					private ModuleDeploymentProperties getModuleDeploymentProperties(ModuleDescriptor moduleDescriptor) {
						ModuleDescriptor.Key key = moduleDescriptor.createKey();
						ModuleDeploymentProperties properties = mapDeploymentProperties.get(moduleDescriptor.createKey());
						if (properties == null) {
							properties = DeploymentPropertiesUtility.createModuleDeploymentProperties(
									stream.getDeploymentProperties(), moduleDescriptor);
							mapDeploymentProperties.put(key, properties);
						}
						return properties;
					}

					@Override
					public ModuleDeploymentProperties propertiesForDescriptor(ModuleDescriptor descriptor,
							boolean includeInstanceSpecificProperties) {
						ModuleDeploymentProperties properties = getModuleDeploymentProperties(descriptor);
						if (!includeInstanceSpecificProperties) {
							return properties;
						}

						int moduleIndex = descriptor.getIndex();
						if (moduleIndex > 0) {
							ModuleDescriptor previous = moduleDescriptors.get(moduleIndex - 1);
							ModuleDeploymentProperties previousProperties = getModuleDeploymentProperties(previous);
							// todo: all property keys should be constants
							if (previousProperties.containsKey("producer.partitionKeyExpression")) {
								ModuleDescriptor.Key moduleKey = descriptor.createKey();
								Integer index = mapModuleCount.get(moduleKey);
								if (index == null) {
									index = 0;
								}
								properties.put("consumer.partitionIndex", String.valueOf(index++));
								mapModuleCount.put(moduleKey, index);
							}
						}

						if (properties.containsKey("producer.partitionKeyExpression")) {
							// todo: if this throws IndexOutOfBoundsException,
							// the stream is trying to partition a sink
							ModuleDeploymentProperties nextProperties =
									getModuleDeploymentProperties(moduleDescriptors.get(moduleIndex + 1));

							String count = nextProperties.get("count");
							Assert.hasText(count);
							properties.put("producer.partitionCount", count);
						}

						mapDeploymentProperties.put(descriptor.createKey(), properties);

						logger.warn("properties for {}: {}", descriptor, properties);
						return properties;
					}
				};

		Collection<ModuleDeploymentWriter.Result> results =
				moduleDeploymentWriter.writeDeployment(stream.getDeploymentOrderIterator(), provider);
		moduleDeploymentWriter.validateResults(results);
	}

	/**
	 * Callable that handles events from a {@link org.apache.curator.framework.recipes.cache.PathChildrenCache}. This
	 * allows for the handling of events to be executed in a separate thread from the Curator thread that raises these
	 * events.
	 */
	class EventHandler implements Callable<Void> {

		/**
		 * Curator client.
		 */
		private final CuratorFramework client;

		/**
		 * Event raised from Curator.
		 */
		private final PathChildrenCacheEvent event;

		/**
		 * Construct an {@code EventHandler}.
		 *
		 * @param client curator client
		 * @param event event raised from Curator
		 */
		EventHandler(CuratorFramework client, PathChildrenCacheEvent event) {
			this.client = client;
			this.event = event;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public Void call() throws Exception {
			try {
				switch (event.getType()) {
					case CHILD_ADDED:
						onChildAdded(client, event.getData());
						break;
					default:
						break;
				}
				return null;
			}
			catch (Exception e) {
				logger.error("Exception caught while handling event", e);
				throw e;
			}
		}
	}

}
