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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.cluster.Container;
import org.springframework.xd.dirt.cluster.ContainerEligibilityFilter;
import org.springframework.xd.dirt.container.store.ContainerRepository;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.spark.SparkDriver;

/**
 * @author Ilayaperumal Gopinathan
 */
public class SparkStreamingModuleContainerEligibilityFilter implements ContainerEligibilityFilter {

	@Autowired
	private ContainerRepository containerRepository;


	public List<Container> applyFilter(ModuleDescriptor moduleDescriptor, Iterable<Container> availableContainers) {
		List<Container> containersForDeployment = new ArrayList<Container>();
		for (Container container : availableContainers) {
			ModuleType moduleType = moduleDescriptor.getType();
			if (moduleType.equals(ModuleType.sparkProcessor) || moduleType.equals(ModuleType.sparkSink)) {
				if (!container.getAttributes().containsKey(SparkDriver.SPARK_MODULE_DEPLOYMENT_ATTRIBUTE)) {
					containersForDeployment.add(container);
				}
			}
			else {
				containersForDeployment.add(container);
			}
		}
		return containersForDeployment;
	}

}
