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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.validation.BindException;
import org.springframework.xd.dirt.container.ContainerAttributes;
import org.springframework.xd.dirt.container.store.ContainerAttributesRepository;
import org.springframework.xd.dirt.core.JobDeploymentsPath;
import org.springframework.xd.dirt.core.ModuleDeploymentProperties;
import org.springframework.xd.dirt.core.ModuleDeploymentsPath;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.core.StreamDeploymentsPath;
import org.springframework.xd.dirt.module.ModuleDefinitionRepository;
import org.springframework.xd.dirt.module.ModuleDeployer;
import org.springframework.xd.dirt.module.ModuleDescriptor;
import org.springframework.xd.dirt.stream.ParsingContext;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.stream.XDParser;
import org.springframework.xd.dirt.stream.XDStreamParser;
import org.springframework.xd.dirt.util.MapBytesUtility;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnectionListener;
import org.springframework.xd.module.DeploymentMetadata;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.core.CompositeModule;
import org.springframework.xd.module.core.Module;
import org.springframework.xd.module.core.SimpleModule;
import org.springframework.xd.module.options.ModuleOptions;
import org.springframework.xd.module.options.ModuleOptionsMetadata;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;
import org.springframework.xd.module.options.PrefixNarrowingModuleOptions;
import org.springframework.xd.module.support.ParentLastURLClassLoader;

/**
 * An instance of this class, registered as a bean in the context for a Container, will handle the registration of that
 * Container's metadata with ZooKeeper by creating an ephemeral node. If the {@link ZooKeeperConnection} used by this
 * registrar is closed, that ephemeral node will be eagerly deleted. Since the {@link ZooKeeperConnection} typically has
 * its lifecycle managed by Spring, that would be the normal behavior when the owning {@link ApplicationContext} is
 * itself closed.
 *
 * @author Mark Fisher
 * @author David Turanski
 */
// todo: Rename ContainerServer or ModuleDeployer since it's driven by callbacks and not really a "server".
public class ContainerRegistrar implements ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware,
BeanClassLoaderAware {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(ContainerRegistrar.class);

	/**
	 * Metadata for the current Container.
	 */
	private final ContainerAttributes containerAttributes;

	/**
	 * Repository where {@link ContainerAttributes} are stored.
	 */
	private final ContainerAttributesRepository containerAttributesRepository;

	/**
	 * The ZooKeeperConnection.
	 */
	private final ZooKeeperConnection zkConnection;

	/**
	 * A {@link PathChildrenCacheListener} implementation that handles deployment requests (and deployment removals) for
	 * this container.
	 */
	private final DeploymentListener deploymentListener = new DeploymentListener();

	/**
	 * Watcher for modules deployed to this container under the {@link Paths#STREAMS} location.
	 */
	private final StreamModuleWatcher streamModuleWatcher = new StreamModuleWatcher();

	/**
	 * Watcher for modules deployed to this container under the {@link Paths#JOBS} location.
	 */
	private final JobModuleWatcher jobModuleWatcher = new JobModuleWatcher();

	/**
	 * Cache of children under the deployments path.
	 */
	private volatile PathChildrenCache deployments;

	/**
	 * Utility to convert maps to byte arrays.
	 */
	private final MapBytesUtility mapBytesUtility = new MapBytesUtility();

	/**
	 * ModuleDefinition repository
	 */
	private final ModuleDefinitionRepository moduleDefinitionRepository;

	/**
	 * Module options metadata resolver.
	 */
	private final ModuleOptionsMetadataResolver moduleOptionsMetadataResolver;

	/**
	 * Stream factory.
	 */
	private final StreamFactory streamFactory;

	/**
	 * Map of deployed modules.
	 */
	private final Map<ModuleDescriptorKey, ModuleDescriptor> mapDeployedModules =
			new ConcurrentHashMap<ModuleDescriptorKey, ModuleDescriptor>();

	/**
	 * The ModuleDeployer this container delegates to when deploying a Module.
	 */
	private final ModuleDeployer moduleDeployer;

	/**
	 * The parser for streams and jobs.
	 */
	private final XDParser parser;

	/**
	 * Application context within which this registrar is defined.
	 */
	private volatile ApplicationContext context;

	/**
	 * ClassLoader provided by the ApplicationContext.
	 */
	private volatile ClassLoader parentClassLoader;

	/**
	 * Create an instance that will register the provided {@link ContainerAttributes} whenever the underlying
	 * {@link ZooKeeperConnection} is established. If that connection is already established at the time this instance
	 * receives a {@link ContextRefreshedEvent}, the attributes will be registered then. Otherwise, registration occurs
	 * within a callback that is invoked for connected events as well as reconnected events.
	 *
	 * @param containerAttributes runtime and configured attributes for the container
	 * @param streamDefinitionRepository repository for streams
	 * @param moduleDefinitionRepository repository for modules
	 * @param moduleOptionsMetadataResolver resolver for module options metadata
	 * @param moduleDeployer module deployer
	 * @param zkConnection ZooKeeper connection
	 */
	public ContainerRegistrar(ContainerAttributes containerAttributes,
			ContainerAttributesRepository containerAttributesRepository,
			StreamDefinitionRepository streamDefinitionRepository,
			ModuleDefinitionRepository moduleDefinitionRepository,
			ModuleOptionsMetadataResolver moduleOptionsMetadataResolver,
			ModuleDeployer moduleDeployer,
			ZooKeeperConnection zkConnection) {
		this.containerAttributes = containerAttributes;
		this.containerAttributesRepository = containerAttributesRepository;
		this.zkConnection = zkConnection;
		this.moduleDefinitionRepository = moduleDefinitionRepository;
		this.moduleOptionsMetadataResolver = moduleOptionsMetadataResolver;
		this.moduleDeployer = moduleDeployer;
		// todo: the streamFactory should be injected
		this.streamFactory = new StreamFactory(streamDefinitionRepository, moduleDefinitionRepository,
				moduleOptionsMetadataResolver);
		this.parser = new XDStreamParser(moduleDefinitionRepository, moduleOptionsMetadataResolver);
	}

	/**
	 * Deploy the requested module.
	 *
	 * @param moduleDescriptor descriptor for the module to be deployed
	 */
	private Module deployModule(ModuleDescriptor moduleDescriptor,
			ModuleDeploymentProperties deploymentProperties) {
		logger.info("Deploying module {}", moduleDescriptor);
		ModuleDescriptorKey key = new ModuleDescriptorKey(moduleDescriptor.getGroup(), moduleDescriptor.getType(),
				moduleDescriptor.getModuleLabel());
		mapDeployedModules.put(key, moduleDescriptor);
		ModuleOptions moduleOptions = this.safeModuleOptionsInterpolate(moduleDescriptor);
		Module module = (moduleDescriptor.isComposed()) ? createComposedModule(moduleDescriptor, moduleOptions,
				deploymentProperties)
				: createSimpleModule(moduleDescriptor, moduleOptions, deploymentProperties);
		// todo: rather than delegate, merge ContainerRegistrar itself into and remove most of ModuleDeployer
		this.moduleDeployer.deployAndStore(module, moduleDescriptor);
		return module;
	}

	/**
	 * Undeploy the requested module.
	 *
	 * @param streamName name of the stream for the module
	 * @param moduleType module type
	 * @param moduleLabel module label
	 */
	protected void undeployModule(String streamName, String moduleType, String moduleLabel) {
		ModuleDescriptorKey key = new ModuleDescriptorKey(streamName, ModuleType.valueOf(moduleType), moduleLabel);
		ModuleDescriptor descriptor = mapDeployedModules.get(key);
		if (descriptor == null) {
			// This is logged at trace level because every module undeployment
			// will cause this to be logged. This is because there is a listener
			// on both streams and modules, and the listener implementation for
			// each will remove the module from the other, thus causing
			// this method to be invoked twice per module undeployment.
			logger.trace("Module {} already undeployed", moduleLabel);
		}
		else {
			logger.info("Undeploying module {}", descriptor);
			mapDeployedModules.remove(key);
			this.moduleDeployer.undeploy(descriptor);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.context = applicationContext;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (this.context.equals(event.getApplicationContext())) {
			if (zkConnection.isConnected()) {
				registerWithZooKeeper(zkConnection.getClient());
			}
			zkConnection.addListener(new ContainerAttributesRegisteringZooKeeperConnectionListener());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.parentClassLoader = classLoader;
	}

	/**
	 * Write the Container attributes to ZooKeeper in an ephemeral node under {@code /xd/containers}.
	 */
	private void registerWithZooKeeper(CuratorFramework client) {
		try {
			// Save the container attributes before creating the container
			// deploy path. This is done because the admin leader/supervisor
			// will delete deployment paths for containers that aren't
			// present in the containers path.
			containerAttributesRepository.save(containerAttributes);

			String moduleDeploymentPath = Paths.build(Paths.MODULE_DEPLOYMENTS, containerAttributes.getId());
			Paths.ensurePath(client, moduleDeploymentPath);
			deployments = new PathChildrenCache(client, moduleDeploymentPath, true,
					ThreadUtils.newThreadFactory("DeploymentsPathChildrenCache"));
			deployments.getListenable().addListener(deploymentListener);
			deployments.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

			logger.info("Started container {}", containerAttributes);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * The listener that triggers registration of the container attributes in a ZooKeeper node.
	 */
	private class ContainerAttributesRegisteringZooKeeperConnectionListener implements ZooKeeperConnectionListener {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void onConnect(CuratorFramework client) {
			registerWithZooKeeper(client);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void onDisconnect(CuratorFramework client) {
			try {
				logger.warn(">>> disconnected container: {}", containerAttributes.getId());
				deployments.getListenable().removeListener(deploymentListener);
				deployments.close();

				for (Iterator<ModuleDescriptorKey> iterator = mapDeployedModules.keySet().iterator(); iterator.hasNext();) {
					ModuleDescriptorKey key = iterator.next();
					undeployModule(key.getStream(), key.getType().name(), key.getLabel());
					iterator.remove();
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Event handler for new module deployments.
	 *
	 * @param client curator client
	 * @param data module data
	 */
	private void onChildAdded(CuratorFramework client, ChildData data) {
		ModuleDeploymentsPath moduleDeploymentsPath = new ModuleDeploymentsPath(data.getPath());
		String streamName = moduleDeploymentsPath.getStreamName();
		String moduleType = moduleDeploymentsPath.getModuleType();
		String moduleLabel = moduleDeploymentsPath.getModuleLabel();
		Module module = (ModuleType.job.toString().equals(moduleType)) ? deployJob(client, streamName, moduleLabel)
				: deployStreamModule(client, streamName, moduleType, moduleLabel);
		if (module != null) {
			Map<String, String> map = new HashMap<String, String>();
			CollectionUtils.mergePropertiesIntoMap(module.getProperties(), map);
			byte[] metadata = mapBytesUtility.toByteArray(map);
			try {
				client.create().withMode(CreateMode.EPHEMERAL).forPath(data.getPath() + "/metadata", metadata);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Deploy the requested job.
	 *
	 * @param client curator client
	 * @param jobName job name
	 * @param jobLabel job label
	 * @return Module deployed job module
	 */
	private Module deployJob(CuratorFramework client, String jobName, String jobLabel) {
		logger.info("Deploying job '{}'", jobName);

		String jobPath = new JobDeploymentsPath().setJobName(jobName)
				.setModuleLabel(jobLabel)
				.setContainer(containerAttributes.getId()).build();

		try {
			Map<String, String> map = mapBytesUtility.toMap(client.getData().forPath(Paths.build(Paths.JOBS, jobName)));

			// todo: do we need something like StreamFactory for jobs, or is that overkill?
			String jobModuleName = jobLabel.substring(0, jobLabel.lastIndexOf('-'));
			ModuleDefinition moduleDefinition = this.moduleDefinitionRepository.findByNameAndType(jobModuleName,
					ModuleType.job);
			List<ModuleDescriptor> requests = this.parser.parse(jobName, map.get("definition"),
					ParsingContext.job);
			ModuleDescriptor moduleDescriptor = requests.get(0);
			// todo: support deployment properties for job modules
			Module module = deployModule(moduleDescriptor, new ModuleDeploymentProperties());

			// this indicates that the container has deployed the module
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.forPath(jobPath, mapBytesUtility.toByteArray(Collections.singletonMap("state", "deployed")));

			// set a watch on this module in the job path;
			// if the node is deleted this indicates an undeployment
			client.getData().usingWatcher(jobModuleWatcher).forPath(jobPath);
			return module;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deploy the requested module for a stream.
	 *
	 * @param client curator client
	 * @param streamName name of the stream for the module
	 * @param moduleType module type
	 * @param moduleLabel module label
	 * @return Module deployed stream module
	 */
	private Module deployStreamModule(CuratorFramework client, String streamName, String moduleType, String moduleLabel) {
		logger.info("Deploying module '{}' for stream '{}'", moduleLabel, streamName);

		String streamPath = new StreamDeploymentsPath().setStreamName(streamName)
				.setModuleType(moduleType)
				.setModuleLabel(moduleLabel)
				.setContainer(containerAttributes.getId()).build();

		Module module = null;
		try {
			Stream stream = streamFactory.createStream(streamName,
					mapBytesUtility.toMap(client.getData().forPath(Paths.build(Paths.STREAMS, streamName))));

			ModuleDescriptor descriptor = stream.getModuleDescriptor(moduleLabel, moduleType);
			ModuleDeploymentProperties moduleDeploymentProperties = new ModuleDeploymentProperties();
			for (String key : stream.getDeploymentProperties().keySet()) {
				String prefix = String.format("module.%s.", descriptor.getModuleName());
				if (key.startsWith(prefix)) {
					moduleDeploymentProperties.put(key.substring(prefix.length()),
							stream.getDeploymentProperties().get(key));
				}
			}

			module = deployModule(descriptor, moduleDeploymentProperties);

			// this indicates that the container has deployed the module
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.forPath(streamPath, mapBytesUtility.toByteArray(Collections.singletonMap("state", "deployed")));

			// set a watch on this module in the stream path;
			// if the node is deleted this indicates an undeployment
			client.getData().usingWatcher(streamModuleWatcher).forPath(streamPath);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (KeeperException.NodeExistsException e) {
			// todo: review, this should not happen
			logger.info("Module for stream {} already deployed", moduleLabel, streamName);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return module;
	}

	/**
	 * Event handler for deployment removals.
	 *
	 * @param client curator client
	 * @param data module data
	 */
	private void onChildRemoved(CuratorFramework client, ChildData data) throws Exception {
		ModuleDeploymentsPath moduleDeploymentsPath = new ModuleDeploymentsPath(data.getPath());
		String streamName = moduleDeploymentsPath.getStreamName();
		String moduleType = moduleDeploymentsPath.getModuleType();
		String moduleLabel = moduleDeploymentsPath.getModuleLabel();

		undeployModule(streamName, moduleType, moduleLabel);

		String path;
		if (ModuleType.job.toString().equals(moduleType)) {
			path = new JobDeploymentsPath().setJobName(streamName)
					.setModuleLabel(moduleLabel)
					.setContainer(containerAttributes.getId()).build();
		}
		else {
			path = new StreamDeploymentsPath().setStreamName(streamName)
					.setModuleType(moduleType)
					.setModuleLabel(moduleLabel)
					.setContainer(containerAttributes.getId()).build();
		}
		if (client.checkExists().forPath(path) != null) {
			logger.trace("Deleting path: {}", path);
			client.delete().forPath(path);
		}
	}

	/**
	 * Create a composed module based on the provided {@link ModuleDescriptor}, {@link ModuleOptions}, and
	 * {@link ModuleDeploymentProperties}.
	 *
	 * @param compositeDescriptor descriptor for the composed module
	 * @param options module options for the composed module
	 * @param deploymentProperties deployment related properties for the composed module
	 *
	 * @return new composed module instance
	 *
	 * @see ModuleDescriptor#isComposed
	 */
	private Module createComposedModule(ModuleDescriptor compositeDescriptor,
			ModuleOptions options, ModuleDeploymentProperties deploymentProperties) {
		String streamName = compositeDescriptor.getGroup();
		int index = compositeDescriptor.getIndex();
		String sourceChannelName = compositeDescriptor.getSourceChannelName();
		String sinkChannelName = compositeDescriptor.getSinkChannelName();

		List<ModuleDescriptor> children = compositeDescriptor.getChildren();
		Assert.notEmpty(children, "child module list must not be empty");

		List<Module> childrenModules = new ArrayList<Module>(children.size());
		for (ModuleDescriptor childRequest : children) {
			ModuleOptions narrowedOptions = new PrefixNarrowingModuleOptions(options, childRequest.getModuleName());
			// due to parser results being reversed, we add each at index 0
			// todo: is it right to pass the composite deploymentProperties here?
			childrenModules.add(0, createSimpleModule(childRequest, narrowedOptions, deploymentProperties));
		}
		DeploymentMetadata metadata = new DeploymentMetadata(streamName, index, sourceChannelName, sinkChannelName);
		return new CompositeModule(compositeDescriptor.getModuleName(), compositeDescriptor.getType(),
				childrenModules, metadata);
	}

	/**
	 * Create a module based on the provided {@link ModuleDescriptor}, {@link ModuleOptions}, and
	 * {@link ModuleDeploymentProperties}.
	 *
	 * @param descriptor descriptor for the module
	 * @param options module options for the module
	 * @param deploymentProperties deployment related properties for the module
	 *
	 * @return new module instance
	 */
	private Module createSimpleModule(ModuleDescriptor descriptor, ModuleOptions options,
			ModuleDeploymentProperties deploymentProperties) {
		String streamName = descriptor.getGroup();
		int index = descriptor.getIndex();
		String sourceChannelName = descriptor.getSourceChannelName();
		String sinkChannelName = descriptor.getSinkChannelName();
		DeploymentMetadata metadata = new DeploymentMetadata(streamName, index, sourceChannelName, sinkChannelName);
		ModuleDefinition definition = descriptor.getModuleDefinition();
		ClassLoader classLoader = (definition.getClasspath() == null) ? null
				: new ParentLastURLClassLoader(definition.getClasspath(), parentClassLoader);
		return new SimpleModule(definition, metadata, classLoader, options);
	}

	/**
	 * Takes a request and returns an instance of {@link ModuleOptions} bound with the request parameters. Binding is
	 * assumed to not fail, as it has already been validated on the admin side.
	 *
	 * @param descriptor module descriptor for which to bind request parameters
	 *
	 * @return module options bound with request parameters
	 */
	private ModuleOptions safeModuleOptionsInterpolate(ModuleDescriptor descriptor) {
		Map<String, String> parameters = descriptor.getParameters();
		ModuleOptionsMetadata moduleOptionsMetadata = moduleOptionsMetadataResolver.resolve(descriptor.getModuleDefinition());
		try {
			return moduleOptionsMetadata.interpolate(parameters);
		}
		catch (BindException e) {
			// Can't happen as parser should have already validated options
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Key to be used in Map of ModuleDescriptors.
	 */
	// todo: should this move into ModuleDescriptor as static Key class?
	class ModuleDescriptorKey implements Comparable<ModuleDescriptorKey> {

		/**
		 * Stream name.
		 */
		private final String stream;

		/**
		 * Module type.
		 */
		private final ModuleType type;

		/**
		 * Module label.
		 */
		private final String label;

		/**
		 * Construct a key.
		 *
		 * @param stream stream name
		 * @param type module type
		 * @param label module label
		 */
		public ModuleDescriptorKey(String stream, ModuleType type, String label) {
			Assert.notNull(stream, "Stream is required");
			Assert.notNull(type, "Type is required");
			Assert.hasText(label, "Label is required");
			this.stream = stream;
			this.type = type;
			this.label = label;
		}

		/**
		 * Return the name of the stream.
		 *
		 * @return stream name
		 */
		public String getStream() {
			return stream;
		}

		/**
		 * Return the module type.
		 *
		 * @return module type
		 */
		public ModuleType getType() {
			return type;
		}

		/**
		 * Return the module label.
		 *
		 * @return module label
		 */
		public String getLabel() {
			return label;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int compareTo(ModuleDescriptorKey other) {
			int c = type.compareTo(other.getType());
			if (c == 0) {
				c = label.compareTo(other.getLabel());
			}
			return c;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (o instanceof ModuleDescriptorKey) {
				ModuleDescriptorKey other = (ModuleDescriptorKey) o;
				return stream.equals(other.getStream())
						&& type.equals(other.getType()) && label.equals(other.getLabel());
			}

			return false;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {
			int result = stream.hashCode();
			result = 31 * result + type.hashCode();
			result = 31 * result + label.hashCode();
			return result;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String toString() {
			return "ModuleDeploymentKey{" +
					"stream='" + stream + '\'' +
					", type=" + type +
					", label='" + label + '\'' +
					'}';
		}

	}

	/**
	 * Watcher for the modules deployed to this container under the {@link Paths#STREAMS} location. If the node is
	 * deleted, this container will undeploy the module.
	 */
	class StreamModuleWatcher implements CuratorWatcher {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void process(WatchedEvent event) throws Exception {
			if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
				StreamDeploymentsPath streamDeploymentsPath = new StreamDeploymentsPath(event.getPath());

				String streamName = streamDeploymentsPath.getStreamName();
				String moduleType = streamDeploymentsPath.getModuleType();
				String moduleLabel = streamDeploymentsPath.getModuleLabel();

				undeployModule(streamName, moduleType, moduleLabel);

				String deploymentPath = new ModuleDeploymentsPath()
						.setContainer(containerAttributes.getId())
						.setStreamName(streamName)
						.setModuleType(moduleType)
						.setModuleLabel(moduleLabel).build();

				CuratorFramework client = zkConnection.getClient();
				try {
					if (client.checkExists().forPath(deploymentPath) != null) {
						logger.trace("Deleting path: {}", deploymentPath);
						client.delete().deletingChildrenIfNeeded().forPath(deploymentPath);
					}
				}
				catch (Exception e) {
					// it is common for a process shutdown to trigger this
					// event; therefore any exception thrown while attempting
					// to delete a deployment path will only be rethrown
					// if the client is in a connected/started state
					if (client.getState() == CuratorFrameworkState.STARTED) {
						throw new RuntimeException(e);
					}
				}
			}
			else {
				logger.debug("Unexpected event {}, ZooKeeper state: {}", event.getType(), event.getState());
				if (EnumSet.of(Watcher.Event.KeeperState.SyncConnected,
						Watcher.Event.KeeperState.SaslAuthenticated,
						Watcher.Event.KeeperState.ConnectedReadOnly).contains(event.getState())) {
					// this watcher is only interested in deletes for the purposes of undeploying modules;
					// if any other change occurs the watch needs to be reestablished
					zkConnection.getClient().getData().usingWatcher(this).forPath(event.getPath());
				}
			}
		}
	}

	/**
	 * Watcher for the modules deployed to this container under the {@link Paths#JOBS} location. If the node is deleted,
	 * this container will undeploy the module.
	 */
	class JobModuleWatcher implements CuratorWatcher {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void process(WatchedEvent event) throws Exception {
			if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
				JobDeploymentsPath jobDeploymentsPath = new JobDeploymentsPath(event.getPath());
				String jobName = jobDeploymentsPath.getJobName();
				String moduleLabel = jobDeploymentsPath.getModuleLabel();

				undeployModule(jobName, ModuleType.job.toString(), moduleLabel);

				String deploymentPath = new ModuleDeploymentsPath()
						.setContainer(containerAttributes.getId())
						.setStreamName(jobName)
						.setModuleType(ModuleType.job.toString())
						.setModuleLabel(moduleLabel).build();

				CuratorFramework client = zkConnection.getClient();
				try {
					if (client.checkExists().forPath(deploymentPath) != null) {
						logger.trace("Deleting path: {}", deploymentPath);
						client.delete().deletingChildrenIfNeeded().forPath(deploymentPath);
					}
				}
				catch (KeeperException e) {
					// it is common for a process shutdown to trigger this
					// event; therefore any exception thrown while attempting
					// to delete a deployment path will only be rethrown
					// if the client is in a connected/started state
					if (client.getState() == CuratorFrameworkState.STARTED) {
						throw new RuntimeException(e);
					}
				}
			}
			else {
				logger.debug("Unexpected event {}, ZooKeeper state: {}", event.getType(), event.getState());
				if (EnumSet.of(Watcher.Event.KeeperState.SyncConnected,
						Watcher.Event.KeeperState.SaslAuthenticated,
						Watcher.Event.KeeperState.ConnectedReadOnly).contains(event.getState())) {
					// this watcher is only interested in deletes for the purposes of undeploying modules;
					// if any other change occurs the watch needs to be reestablished
					zkConnection.getClient().getData().usingWatcher(this).forPath(event.getPath());
				}
			}
		}
	}

	/**
	 * Listener for deployment requests for this container under {@link Paths#DEPLOYMENTS}.
	 */
	class DeploymentListener implements PathChildrenCacheListener {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			logger.warn("Path cache event: {}", event);
			switch (event.getType()) {
				case INITIALIZED:
					break;
				case CHILD_ADDED:
					onChildAdded(client, event.getData());
					break;
				case CHILD_REMOVED:
					onChildRemoved(client, event.getData());
					break;
				default:
					break;
			}
		}
	}

}
