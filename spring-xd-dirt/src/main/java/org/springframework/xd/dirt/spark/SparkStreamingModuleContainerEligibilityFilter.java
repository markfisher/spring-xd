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
package org.springframework.xd.dirt.spark;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BindException;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.ContainerEligibilityFilter;
import org.springframework.xd.dirt.module.store.ModuleMetadata;
import org.springframework.xd.dirt.module.store.ModuleMetadataRepository;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.core.ModuleFactory;
import org.springframework.xd.module.options.ModuleOptions;
import org.springframework.xd.module.options.ModuleOptionsMetadata;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;
import org.springframework.xd.module.spark.SparkModule;

/**
 * @author Ilayaperumal Gopinathan
 */
public class SparkStreamingModuleContainerEligibilityFilter implements ContainerEligibilityFilter {

	@Autowired
	private ModuleMetadataRepository moduleMetadataRepository;

	@Autowired
	private ModuleOptionsMetadataResolver moduleOptionsMetadataResolver;


	public List<Container> applyFilter(ModuleDescriptor moduleDescriptor, Iterable<Container> availableContainers) {
		ModuleOptions options;
		ModuleOptionsMetadata optionsMetadata = moduleOptionsMetadataResolver.resolve(moduleDescriptor.getModuleDefinition());
		List<Container> containersForDeployment = Lists.newArrayList(availableContainers);
		try {
			options = optionsMetadata.interpolate(moduleDescriptor.getParameters());
			String name = (String) options.asPropertySource().getProperty(ModuleFactory.MODULE_EXECUTION_FRAMEWORK);
			if (name != null &&  (name.equals(SparkModule.MODULE_EXECUTION_FRAMEWORK))) {
				Iterable<ModuleMetadata> deployedModules = moduleMetadataRepository.findAll();
				List<ModuleMetadata> sparkModules = new ArrayList<ModuleMetadata>();
				for (ModuleMetadata moduleMetadata : deployedModules) {
					String moduleExecutionFramework =
							moduleMetadata.getModuleOptions().getProperty(ModuleFactory.MODULE_EXECUTION_FRAMEWORK);
					if (moduleExecutionFramework != null &&
							(moduleExecutionFramework.equals(SparkModule.MODULE_EXECUTION_FRAMEWORK))) {
						sparkModules.add(moduleMetadata);
					}
				}
				for (Container container : availableContainers) {
					for (ModuleMetadata sparkModule : sparkModules) {
						if (sparkModule.getContainerId().equals(container.getName())) {
							containersForDeployment.remove(container);
						}
					}
				}
			}
		}
		catch (BindException be) {
			throw new RuntimeException(be);
		}
		return containersForDeployment;
	}
}
