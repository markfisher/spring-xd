/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.xd.dirt.module;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.Message;
import org.springframework.integration.message.GenericMessage;
import org.springframework.xd.dirt.server.options.OptionUtils;
import org.springframework.xd.dirt.server.options.SingleNodeOptions;
import org.springframework.xd.module.Module;
import org.springframework.xd.module.Plugin;


/**
 * 
 * @author David Turanski
 */
public class ModuleDeployerTests {

	private ModuleDeployer moduleDeployer;

	@Before
	public void setUp() {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
		context.setConfigLocation(
				"/org/springframework/xd/dirt/module/ModuleDeployerTests-context.xml");
		OptionUtils.configureRuntime(new SingleNodeOptions(), context.getEnvironment());
		context.refresh();
		moduleDeployer = context.getBean(ModuleDeployer.class);
	}


	@Test
	public void testModuleContext() {
		ModuleDeploymentRequest request = new ModuleDeploymentRequest();
		request.setGroup("test");
		request.setIndex(0);
		request.setType("sink");
		request.setModule("log");
		Message<ModuleDeploymentRequest> message = new GenericMessage<ModuleDeploymentRequest>(request);
		moduleDeployer.handleMessage(message);
	}

	public static class TestPlugin implements Plugin {

		private ApplicationContext moduleCommonContext;

		@Override
		public void preProcessModule(Module module) {
			assertEquals("module commonContext should not contain any Plugins", 0,
					moduleCommonContext.getBeansOfType(Plugin.class).size());
			Properties properties = new Properties();
			properties.setProperty("xd.stream.name", module.getDeploymentMetadata().getGroup());
			module.addProperties(properties);
		}

		@Override
		public void postProcessModule(Module module) {
		}

		@Override
		public void preDestroyModule(Module module) {
		}

		@Override
		public void removeModule(Module module) {

		}

		@Override
		public void preProcessSharedContext(ConfigurableApplicationContext moduleCommonContext) {
			this.moduleCommonContext = moduleCommonContext;
		}
	}

}
