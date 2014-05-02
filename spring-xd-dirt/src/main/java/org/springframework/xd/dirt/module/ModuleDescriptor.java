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

package org.springframework.xd.dirt.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.core.style.ToStringCreator;
import org.springframework.util.Assert;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleType;

/**
 * Representation of a module in the context of a defined stream or job.
 * TODO: this will be renamed to ModuleDescriptor.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Luke Taylor
 * @author Ilayaperumal Gopinathan
 * @author Patrick Peralta
 */
public class ModuleDescriptor implements Comparable<ModuleDescriptor> {

	/**
	 * Name of module. Typically this module is present under
	 * {@code $XD_HOME/modules/[module type]}.
	 */
	private final String moduleName;

	/**
	 * Symbolic name of a module. This may be generated to a default value
	 * or specified in the DSL string.
	 */
	private final String moduleLabel;

	/**
	 * Name of deployable unit this module instance belongs to (such as a
	 * stream or job).
	 */
	private final String group;

	/**
	 * Name of source channel, if defined by the stream/job definition.
	 * May be {@code null}.
	 */
	private final String sourceChannelName;

	/**
	 * Name of sink channel, if defined by the stream/job definition.
	 * May be {@code null}.
	 */
	private final String sinkChannelName;

	/**
	 * Position in stream/job definition relative to the other modules
	 * in the definition. 0 indicates the first/leftmost position.
	 */
	private final int index;

	/**
	 * Module type.
	 */
	private final ModuleType type;

	/**
	 * Parameters for module. This is specific to the type of module - for instance
	 * an http module would include a port number as a parameter.
	 */
	private final Map<String, String> parameters;

	/**
	 * If this is a composite module, this list contains the modules that
	 * this module consists of; otherwise this list is empty.
	 */
	private final List<ModuleDescriptor> children;

	private final ModuleDefinition moduleDefinition;


	/**
	 * Construct a {@code ModuleDeploymentRequest}. This constructor is
	 * private; use {@link org.springframework.xd.dirt.module.ModuleDescriptor.Builder}
	 * to create a new instance.
	 *
	 * @param moduleName         name of module
	 * @param moduleLabel        label used for module in stream/job definition
	 * @param group              group this module belongs to (stream/job)
	 * @param sourceChannelName  name of source channel; may be {@code null}
	 * @param sinkChannelName    name of sink channel; may be {@code null}
	 * @param index              position of module in stream/job definition
	 * @param type               module type
	 * @parma moduleDefinition   module definition
	 * @param parameters         module parameters; may be {@code null}
	 * @param children           if this is a composite module, this list contains
	 *                           the modules that comprise this module; may be {@code null}
	 */
	private ModuleDescriptor(String moduleName, String moduleLabel,
			String group, String sourceChannelName, String sinkChannelName,
			int index, ModuleType type, ModuleDefinition moduleDefinition,
			Map<String, String> parameters, List<ModuleDescriptor> children) {
		this.moduleName = moduleName;
		this.moduleLabel = moduleLabel;
		this.group = group;
		this.sourceChannelName = sourceChannelName;
		this.sinkChannelName = sinkChannelName;
		this.index = index;
		this.type = type;
		this.moduleDefinition = moduleDefinition;
		this.parameters = parameters == null
				? Collections.<String, String>emptyMap()
				: Collections.unmodifiableMap(new HashMap<String, String>(parameters));
		this.children = children == null
				? Collections.<ModuleDescriptor>emptyList()
				: Collections.unmodifiableList(new ArrayList<ModuleDescriptor>(children));
	}

	/**
	 * Return name of module. Typically this module is present under
	 * {@code $XD_HOME/modules/[module type]}.
	 *
	 * @return module name
	 */
	public String getModuleName() {
		return moduleName;
	}

	/**
	 * Return symbolic name of a module. This may be generated to a default value
	 * or specified in the DSL string.
	 *
	 * @return module label
	 */
	public String getModuleLabel() {
		return moduleLabel;
	}

	/**
	 * Return name of deployable unit this module instance belongs to (such as a
	 * stream or job).
	 *
	 * @return group name
	 */
	public String getGroup() {
		return group;
	}

	/**
	 * Return position in stream/job definition relative to the other modules
	 * in the definition. 0 indicates the first/leftmost position.
	 *
	 * @return module index
	 */
	public int getIndex() {
		return index;
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
	 * Return name of source channel, if defined by the stream/job definition.
	 * May be null.
	 *
	 * @return source channel name, or {@code null} if no source channel defined
	 */
	public String getSourceChannelName() {
		return sourceChannelName;
	}

	/**
	 * Return name of sink channel, if defined by the stream/job definition.
	 * May be null.
	 *
	 * @return sink channel name, or {@code null} if no sink channel defined
	 */
	public String getSinkChannelName() {
		return sinkChannelName;
	}

	/**
	 * Return parameters for module. This is specific to the type of module - for instance
	 * an http module would include a port number as a parameter.
	 *
	 * @return read-only map of module parameters
	 */
	public Map<String, String> getParameters() {
		return this.parameters;
	}

	/**
	 * If this is a composite module, this list contains the modules that
	 * this module consists of; otherwise this list is empty.
	 *
	 * @return sub modules for this module, or empty list if this
	 *         is not a composite module
	 */
	public List<ModuleDescriptor> getChildren() {
		return children;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return new ToStringCreator(this)
				.append("moduleName", moduleName)
				.append("moduleLabel", moduleLabel)
				.append("group", group)
				.append("sourceChannelName", sourceChannelName)
				.append("sinkChannelName", sinkChannelName)
				.append("sinkChannelName", sinkChannelName)
				.append("index", index)
				.append("type", type)
				.append("parameters", parameters)
				.append("children", children).toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(ModuleDescriptor o) {
		Assert.notNull(o, "ModuleDeploymentRequest must not be null");
		return Integer.compare(index, o.index);
	}

	/**
	 * todo
	 * @return
	 */
	public boolean isComposed() {
		return !children.isEmpty();
	}

	public ModuleDefinition getModuleDefinition() {
		return moduleDefinition;
	}

	/**
	 * Builder object for {@link org.springframework.xd.dirt.module.ModuleDescriptor}.
	 * This object is mutable to allow for flexibility in specifying module type/fields/parameters
	 * during parsing.
	 */
	public static class Builder {

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#moduleName
		 */
		private String moduleName;

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#moduleLabel
		 */
		private String moduleLabel;

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#group
		 */
		private String group;

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#sourceChannelName
		 */
		private String sourceChannelName;

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#sinkChannelName
		 */
		private String sinkChannelName;

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#index
		 */
		private int index;

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#type
		 */
		private ModuleType type;

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#parameters
		 */
		private final Map<String, String> parameters = new HashMap<String, String>();

		/**
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#children
		 */
		private final List<ModuleDescriptor> children = new ArrayList<ModuleDescriptor>();

		private ModuleDefinition moduleDefinition;

		/**
		 * Set the module name.
		 *
		 * @param moduleName name of module
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#moduleName
		 */
		public Builder setModuleName(String moduleName) {
			this.moduleName = moduleName;
			return this;
		}

		/**
		 * Set the module label.
		 *
		 * @param moduleLabel name of module label
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#moduleLabel
		 */
		public Builder setModuleLabel(String moduleLabel) {
			this.moduleLabel = moduleLabel;
			return this;
		}

		/**
		 * Set the module group.
		 *
		 * @param group name of module group
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#group
		 */
		public Builder setGroup(String group) {
			this.group = group;
			return this;
		}

		/**
		 * Set the module source channel name.
		 *
		 * @param sourceChannelName name of source channel; may be {@code null}
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#sourceChannelName
		 */
		public Builder setSourceChannelName(String sourceChannelName) {
			this.sourceChannelName = sourceChannelName;
			return this;
		}

		/**
		 * Set the module sink channel name.
		 *
		 * @param sinkChannelName name of sink channel; may be {@code null}
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#sinkChannelName
		 */
		public Builder setSinkChannelName(String sinkChannelName) {
			this.sinkChannelName = sinkChannelName;
			return this;
		}

		/**
		 * Set the module index.
		 *
		 * @param index position of module in stream/job definition
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#index
		 */
		public Builder setIndex(int index) {
			this.index = index;
			return this;
		}

		/**
		 * Set the module type.
		 *
		 * @param type module type
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#type
		 */
		public Builder setType(ModuleType type) {
			this.type = type;
			return this;
		}

		/**
		 * Add the list of children to the list of sub modules. This only
		 * applies if this builder is for a composite module.
		 *
		 * @param children sub modules
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#children
		 */
		public Builder addChildren(List<ModuleDescriptor> children) {
			this.children.addAll(children);
			return this;
		}

		/**
		 * Set a module parameter.
		 *
		 * @param name   parameter name
		 * @param value  parameter value
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#parameters
		 */
		public Builder setParameter(String name, String value) {
			this.parameters.put(name, value);
			return this;
		}

		/**
		 * Add the contents of the provided map to the map of module parameters.
		 *
		 * @param parameters module parameters
		 * @return this builder object
		 *
		 * @see org.springframework.xd.dirt.module.ModuleDescriptor#parameters
		 */
		public Builder addParameters(Map<String, String> parameters) {
			this.parameters.putAll(parameters);
			return this;
		}

		/**
		 * Return name of module. Typically this module is present under
		 * {@code $XD_HOME/modules/[module type]}.
		 *
		 * @return module name
		 */
		public String getModuleName() {
			return moduleName;
		}

		/**
		 * Return symbolic name of a module. This may be generated to a default value
		 * or specified in the DSL string.
		 *
		 * @return module label
		 */
		public String getModuleLabel() {
			return moduleLabel;
		}

		/**
		 * Return name of deployable unit this module instance belongs to (such as a
		 * stream or job).
		 *
		 * @return group name
		 */
		public String getGroup() {
			return group;
		}

		/**
		 * Return name of source channel, if defined by the stream/job definition.
		 * May be null.
		 *
		 * @return source channel name, or {@code null} if no source channel defined
		 */
		public String getSourceChannelName() {
			return sourceChannelName;
		}

		/**
		 * Return name of sink channel, if defined by the stream/job definition.
		 * May be null.
		 *
		 * @return sink channel name, or {@code null} if no sink channel defined
		 */
		public String getSinkChannelName() {
			return sinkChannelName;
		}

		/**
		 * Return position in stream/job definition relative to the other modules
		 * in the definition. 0 indicates the first/leftmost position.
		 *
		 * @return module index
		 */
		public int getIndex() {
			return index;
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
		 * Return parameters for module. This is specific to the type of module - for instance
		 * an http module would include a port number as a parameter.
		 * <br />
		 * Note that the contents of this map are <b>mutable</b>.
		 *
		 * @return map of module parameters
		 */
		public Map<String, String> getParameters() {
			return parameters;
		}

		/**
		 * Create a {@code Builder} object pre-populated with the configuration
		 * for the provided {@link org.springframework.xd.dirt.module.ModuleDescriptor}.
		 *
		 * @param request module descriptor
		 * @return pre-populated builder object
		 */
		public static Builder fromModuleDeploymentRequest(ModuleDescriptor request) {
			Builder builder = new Builder();
			builder.setModuleName(request.getModuleName());
			builder.setModuleLabel(request.getModuleLabel());
			builder.setGroup(request.getGroup());
			builder.setSourceChannelName(request.getSourceChannelName());
			builder.setSinkChannelName(request.getSinkChannelName());
			builder.setIndex(request.getIndex());
			builder.setType(request.getType());
			builder.setModuleDefinition(request.getModuleDefinition());
			builder.addParameters(request.getParameters());
			builder.addChildren(request.getChildren());

			return builder;
		}

		/**
		 * Return a new instance of {@link org.springframework.xd.dirt.module.ModuleDescriptor}.
		 *
		 * @return new instance of {@code ModuleDeploymentRequest}
		 */
		public ModuleDescriptor build() {
			return new ModuleDescriptor(moduleName, moduleLabel, group,
					sourceChannelName, sinkChannelName, index, type, moduleDefinition,
					parameters, children);
		}

		public Builder setModuleDefinition(ModuleDefinition moduleDefinition) {
			this.moduleDefinition = moduleDefinition;
			return this;
		}

		public ModuleDefinition getModuleDefinition() {
			return moduleDefinition;
		}
	}
}
