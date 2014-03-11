/*
 * Copyright 2013-2014 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;

import org.springframework.boot.autoconfigure.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.core.OrderComparator;
import org.springframework.xd.dirt.container.initializer.OrderedContextInitializer;
import org.springframework.xd.dirt.server.options.CommandLinePropertySourceOverridingListener;
import org.springframework.xd.dirt.server.options.CommonOptions;

/**
 * Package private class to bootstrap the Container process. Configures and instantiates
 * {@link OrderedContextInitializer}s and provides them to create the main container context.
 * 
 * @author David Turanski
 */
class ContainerBootstrapContext {

	private CommandLinePropertySourceOverridingListener<?> commandLineListener;

	private ApplicationListener<?>[] orderedContextInitializers;

	<T extends CommonOptions> ContainerBootstrapContext(T options) {

		commandLineListener =
				new CommandLinePropertySourceOverridingListener<T>(options);

		ApplicationContext bootstrapContext = new SpringApplicationBuilder(ContainerBootstrapConfiguration.class,
				options.getClass(),
				PropertyPlaceholderAutoConfiguration.class)
				.listeners(commandLineListener)
				.headless(true)
				.web(false)
				.run();

		Collection<OrderedContextInitializer> orderedContextInitializerBeans = bootstrapContext.getBeansOfType(
				OrderedContextInitializer.class).values();

		this.orderedContextInitializers = orderedContextInitializerBeans.toArray(new
				ApplicationListener<?>[orderedContextInitializerBeans.size()]);
		Arrays.sort(orderedContextInitializers, new OrderComparator());
	}

	ApplicationListener<?>[] orderedContextInitializers() {
		return this.orderedContextInitializers;
	}

	CommandLinePropertySourceOverridingListener<?> commandLineListener() {
		return this.commandLineListener;
	}
}
