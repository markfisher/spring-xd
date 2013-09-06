/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.xd.dirt.plugins.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.x.channel.registry.ChannelRegistry;
import org.springframework.xd.module.BeanDefinitionAddingPostProcessor;
import org.springframework.xd.module.DeploymentMetadata;
import org.springframework.xd.module.Module;
import org.springframework.xd.module.ModuleDefinition;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.SimpleModule;

/**
 * @author Mark Fisher
 * @author Jennifer Hickey
 * @author Gary Russell
 */
public class StreamPluginTests {

	private StreamPlugin plugin = new StreamPlugin();

	private MessageChannel input = new DirectChannel();

	private MessageChannel output = new DirectChannel();

	private MessageChannel tap = new DirectChannel();

	@Before
	public void setup() {
		System.setProperty("XD_TRANSPORT", "local");
	}

	@Test
	public void streamPropertiesAdded() {
		Module module = new SimpleModule(new ModuleDefinition("testsource", "source"), new DeploymentMetadata("foo", 0));
		assertEquals(0, module.getProperties().size());
		plugin.preProcessModule(module);
		plugin.postProcessModule(module);
		assertEquals(2, module.getProperties().size());
		assertEquals("foo", module.getProperties().getProperty("xd.stream.name"));
		assertEquals("0", module.getProperties().getProperty("xd.module.index"));
	}

	@Test
	public void streamChannelTests() {
		Module module = mock(Module.class);
		when(module.getDeploymentMetadata()).thenReturn(new DeploymentMetadata("foo", 1));
		when(module.getType()).thenReturn(ModuleType.PROCESSOR.toString());
		final ChannelRegistry registry = mock(ChannelRegistry.class);
		when(module.getName()).thenReturn("testing");
		when(module.getComponent(ChannelRegistry.class)).thenReturn(registry);
		when(module.getComponent("input", MessageChannel.class)).thenReturn(input);
		when(module.getComponent("output", MessageChannel.class)).thenReturn(output);
		when(module.getComponent("tap", MessageChannel.class)).thenReturn(tap);
		plugin.preProcessModule(module);
		plugin.postProcessModule(module);
		verify(registry).createInbound("foo.0", input, Collections.singletonList(MediaType.ALL), false);
		verify(registry).createOutbound("foo.1", output, false);
		verify(registry).createOutboundPubSub("tap:foo.testing", tap);
		plugin.preDestroyModule(module);
		plugin.removeModule(module);
		verify(registry).deleteInbound("foo.0");
		verify(registry).deleteOutbound("foo.1");
		verify(registry).deleteOutboundPubSub("tap:foo.testing", tap);
	}

	@Test
	public void sharedComponentsAdded() {
		GenericApplicationContext context = new GenericApplicationContext();
		plugin.preProcessSharedContext(context);
		List<BeanFactoryPostProcessor> sharedBeans = context.getBeanFactoryPostProcessors();
		assertEquals(1, sharedBeans.size());
		assertTrue(sharedBeans.get(0) instanceof BeanDefinitionAddingPostProcessor);
	}

}
