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

package org.springframework.xd.dirt.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.xd.dirt.module.ModuleDeploymentRequest;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.core.Module;

//todo: decide if a Stream really should provide ContainerMatcher (as done in the prototype)

/**
 * Domain model for an XD Stream. A stream consists of a set of modules used to process the flow of data.
 *
 * @author Patrick Peralta
 */
public class Stream {

	/**
	 * Name of stream.
	 */
	private final String name;

//	/**
//	 * Source module for this stream. This module is responsible for obtaining data for this stream.
//	 */
//	private final ModuleDeploymentRequest source;

//	/**
//	 * Source channel for this stream (only present if no source module).
//	 */
//	private final String sourceChannelName;

//	/**
//	 * Ordered list of processor modules. The data obtained by the source module will be fed to these processors in the
//	 * order indicated by this list.
//	 */
//	private final List<ModuleDeploymentRequest> processors;

//	/**
//	 * Sink module for this stream. This is the ultimate destination for the stream data.
//	 */
//	private final ModuleDeploymentRequest sink;

//	/**
//	 * Sink channel for this stream (only present if no sink module).
//	 */
//	private final String sinkChannelName;

	private final Deque<ModuleDeploymentRequest> descriptors;

	/**
	 * Deployment properties for this stream.
	 */
	private final Map<String, String> deploymentProperties;

	/**
	 * Construct a Stream.
	 *
	 * @param name stream name
	 * @param source source module
	 * @param sourceChannelName source channel
	 * @param processors processor modules
	 * @param sink sink module
	 * @param sinkChannelName sink channel
	 * @param deploymentProperties stream deployment properties
	 */
	private Stream(String name, Deque<ModuleDeploymentRequest> descriptors,  Map<String, String> deploymentProperties) {
		this.name = name;
		this.descriptors = descriptors;
		this.deploymentProperties = deploymentProperties;
	}

	/**
	 * Return the name of this stream.
	 *
	 * @return stream name
	 */
	public String getName() {
		return name;
	}

//	/**
//	 * Return the source module for this stream.
//	 *
//	 * @return source module
//	 */
//	public ModuleDeploymentRequest getSource() {
//		return source;
//	}

	/**
	 * Return the ordered list of processors for this stream.
	 *
	 * @return list of processors
	 */
	public Deque<ModuleDeploymentRequest> getDescriptors() {
		return descriptors;
	}

//	/**
//	 * Return the sink for this stream.
//	 *
//	 * @return sink module
//	 */
//	public ModuleDeploymentRequest getSink() {
//		return sink;
//	}

	/**
	 * Return an iterator that indicates the order of module deployments for this stream. The modules are returned in
	 * reverse order; i.e. the sink is returned first followed by the processors in reverse order followed by the
	 * source.
	 *
	 * @return iterator that iterates over the modules in deployment order
	 */
	public Iterator<ModuleDeploymentRequest> getDeploymentOrderIterator() {
		return descriptors.iterator();
	}

	/**
	 * Return the deployment properties for this stream.
	 *
	 * @return stream deployment properties
	 */
	public Map<String, String> getDeploymentProperties() {
		return deploymentProperties;
	}

//	/**
//	 * Return the module descriptor for the given module label and type.
//	 *
//	 * @param moduleLabel module label
//	 * @param moduleType module type
//	 *
//	 * @return module descriptor
//	 *
//	 * @throws IllegalArgumentException if the module name/type cannot be found
//	 */
//	public ModuleDeploymentRequest getModuleDeploymentRequest(String moduleLabel, String moduleType) {
//		ModuleType type = ModuleType.valueOf(moduleType.toLowerCase());
//		ModuleDeploymentRequest moduleDescriptor = null;
//		switch (type) {
//			case source:
//				moduleDescriptor = getSource();
//				break;
//			case sink:
//				moduleDescriptor = getSink();
//				break;
//			case processor:
//				for (ModuleDeploymentRequest processor : processors) {
//					if (processor.getModuleLabel().equals(moduleLabel)) {
//						moduleDescriptor = processor;
//						break;
//					}
//				}
//		}
//
//		if (moduleDescriptor == null || !moduleLabel.equals(moduleDescriptor.getModuleLabel())) {
//			throw new IllegalArgumentException(String.format("Module %s of type %s not found", moduleLabel, moduleType));
//		}
//		return moduleDescriptor;
//	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Stream{name='" + name + "'}";
	}

	public ModuleDeploymentRequest getModuleDescriptor(String moduleLabel, String moduleType) {
		for (ModuleDeploymentRequest descriptor : descriptors) {
			if (descriptor.getModuleLabel().equals(moduleLabel)
					&& descriptor.getType().toString().equals(moduleType)) {
				return descriptor;
			}
		}
		throw new IllegalStateException(); //todo
	}


	/**
	 * Builder object for {@link Stream} that supports fluent style configuration.
	 */
	public static class Builder {

		/**
		 * Stream name.
		 */
		private String name;

//		/**
//		 * Source channel name.
//		 */
//		private String sourceChannelName;
//
//		/**
//		 * Sink channel name.
//		 */
//		private String sinkChannelName;
//
//		/**
//		 * Map of module labels to module definitions.
//		 */
//		private Map<String, ModuleDefinition> moduleDefinitions = new LinkedHashMap<String, ModuleDefinition>();
//
//		/**
//		 * Map of module labels to module parameters.
//		 */
//		private Map<String, Map<String, String>> moduleParameters = new LinkedHashMap<String, Map<String, String>>();

		/**
		 * Stream deployment properties
		 */
		private Map<String, String> deploymentProperties = Collections.emptyMap();

		private Deque<ModuleDeploymentRequest> moduleDescriptors = new LinkedList<ModuleDeploymentRequest>();

		/**
		 * Set the stream name.
		 *
		 * @param name stream name
		 *
		 * @return this builder
		 */
		public Builder setName(String name) {
			this.name = name;
			return this;
		}

//		/**
//		 * Set the source channel name.
//		 *
//		 * @param sourceChannelName source channel name
//		 *
//		 * @return this builder
//		 */
//		public Builder setSourceChannelName(String sourceChannelName) {
//			Assert.isTrue(moduleDefinitions.isEmpty()
//					|| ModuleType.source != moduleDefinitions.values().iterator().next().getType(),
//					"cannot have both a source module and a source channel");
//			this.sourceChannelName = sourceChannelName;
//			return this;
//		}

//		/**
//		 * Set the sink channel name.
//		 *
//		 * @param sinkChannelName sink channel name
//		 *
//		 * @return this builder
//		 */
//		public Builder setSinkChannelName(String sinkChannelName) {
//			ModuleDefinition lastModuleDefinition = null;
//			for (ModuleDefinition moduleDefinition : moduleDefinitions.values()) {
//				lastModuleDefinition = moduleDefinition;
//			}
//			Assert.isTrue(lastModuleDefinition == null || ModuleType.sink != lastModuleDefinition.getType(),
//					"cannot have both a sink module and a sink channel");
//			this.sinkChannelName = sinkChannelName;
//			return this;
//		}

		public Builder setModuleDescriptors(Collection<ModuleDeploymentRequest> descriptors) {
			this.moduleDescriptors.addAll(descriptors);
			return this;
		}


//		/**
//		 * Add a module definition to this stream builder. Processor modules will be added to the stream in the order
//		 * they are added to this builder.
//		 *
//		 * @param label label for this module
//		 * @param moduleDefinition module definition to add
//		 * @return this builder
//		 */
//		public Builder addModuleDefinition(String label, ModuleDefinition moduleDefinition,
//				Map<String, String> parameters) {
//			if (moduleDefinitions.containsKey(label)) {
//				throw new IllegalArgumentException(String.format("Label %s already in use", label));
//			}
//			if (ModuleType.source == moduleDefinition.getType()) {
//				Assert.isNull(sourceChannelName, "cannot have both a source module and a source channel");
//			}
//			if (ModuleType.sink == moduleDefinition.getType()) {
//				Assert.isNull(sinkChannelName, "cannot have both a sink module and a sink channel");
//			}
//			moduleDefinitions.put(label, moduleDefinition);
//			moduleParameters.put(label, parameters);
//			return this;
//		}

		/**
		 * Set the deployment properties for the stream.
		 *
		 * @param deploymentProperties stream deployment properties
		 *
		 * @return this builder
		 */
		public Builder setDeploymentProperties(Map<String, String> deploymentProperties) {
			this.deploymentProperties = deploymentProperties;
			return this;
		}

		/**
		 * Create a new instance of {@link Stream}.
		 *
		 * @return new Stream instance
		 */
		public Stream build() {
//			List<ModuleDeploymentRequest> processorDescriptors = new ArrayList<ModuleDeploymentRequest>();

			// todo: re-add deployment properties
//			int i = 0;
//			for (Map.Entry<String, ModuleDefinition> entry : moduleDefinitions.entrySet()) {
//				String label = entry.getKey();
//				ModuleDefinition moduleDefinition = entry.getValue();
//				ModuleDeploymentProperties moduleDeploymentProperties = new ModuleDeploymentProperties();
//				for (String key : deploymentProperties.keySet()) {
//					String prefix = String.format("module.%s.", moduleDefinition.getName());
//					if (key.startsWith(prefix)) {
//						moduleDeploymentProperties.put(key.substring(prefix.length()), deploymentProperties.get(key));
//					}
//				}

//				ModuleDeploymentRequest descriptor = new ModuleDeploymentRequest(moduleDefinition, name, label, i, moduleDeploymentProperties);
//				if (!StringUtils.isEmpty(moduleDefinition.getDefinition())) {
//					descriptor.setComposed(true);
//				}
//				// todo: cleanup (and if we do use i here, consider having it in the for loop itself)
//				if (i == moduleDefinitions.size() - 1 && sinkChannelName != null) {
//					descriptor.setSinkChannelName(sinkChannelName);
//				}
//				if (i == 0 && sourceChannelName != null) {
//					descriptor.setSourceChannelName(sourceChannelName);
//				}
//				i++;
//				descriptor.addParameters(moduleParameters.get(label));
//				switch (moduleDefinition.getType()) {
//					case source:
//						sourceDescriptor = descriptor;
//						break;
//					case processor:
//						processorDescriptors.add(descriptor);
//						break;
//					case sink:
//						sinkDescriptor = descriptor;
//				}
//			}

//			Assert.isTrue(sourceDescriptor != null || sourceChannelName != null);
//			Assert.isTrue(sinkDescriptor != null || sinkChannelName != null);

			return new Stream(name, moduleDescriptors, deploymentProperties);
		}

	}

}
