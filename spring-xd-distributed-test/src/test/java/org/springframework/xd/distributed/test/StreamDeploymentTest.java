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

package org.springframework.xd.distributed.test;

import static org.junit.Assert.*;
import static org.springframework.xd.distributed.test.DistributedTestUtils.*;

import java.net.URI;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.oracle.tools.runtime.java.JavaApplication;
import com.oracle.tools.runtime.java.SimpleJavaApplication;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.hateoas.PagedResources;
import org.springframework.util.StringUtils;
import org.springframework.xd.rest.client.domain.ModuleMetadataResource;
import org.springframework.xd.rest.client.domain.StreamDefinitionResource;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;

/**
 * @author Patrick Peralta
 */
public class StreamDeploymentTest {
	private static final Logger logger = LoggerFactory.getLogger(StreamDeploymentTest.class);

	private static TestingServer zooKeeper;

	private static JavaApplication<SimpleJavaApplication> hsqlServer;

	private static JavaApplication<SimpleJavaApplication> adminServer;


	@BeforeClass
	public static void startServers() throws Exception {
		zooKeeper = startZooKeeper();
		hsqlServer = startHsql();
		adminServer = startAdmin();
	}

	@AfterClass
	public static void stopServers() throws Exception {
		if (hsqlServer != null) {
			hsqlServer.close();
		}
		if (adminServer != null) {
			adminServer.close();
		}
		if (zooKeeper != null) {
			zooKeeper.stop();
		}
	}

	@Test
	public void testServers() throws Exception {
		Map<Long, JavaApplication<SimpleJavaApplication>> mapPidContainers = new HashMap<>();

		try {
			for (int i = 0; i < 3; i++) {
				JavaApplication<SimpleJavaApplication> containerServer = startContainer();
				mapPidContainers.put(containerServer.getId(), containerServer);
			}

			SpringXDTemplate template = new SpringXDTemplate(new URI(ADMIN_URL));
			logger.info("Waiting for containers...");
			Map<Long, String> mapPidUuid = waitForContainers(template, mapPidContainers.keySet());
			logger.info("Containers running");

			template.streamOperations().createStream("distributed-ticktock", "time|log", false);
			PagedResources<StreamDefinitionResource> list = template.streamOperations().list();

			Iterator<StreamDefinitionResource> iterator = list.iterator();
			assertTrue(iterator.hasNext());
			StreamDefinitionResource stream = iterator.next();
			logger.info(stream.toString());
			assertEquals("distributed-ticktock", stream.getName());
			assertFalse(iterator.hasNext());

			template.streamOperations().deploy("distributed-ticktock", null);

			// verify modules
			RuntimeContainers moduleContainers = getRuntimeContainers(template);

			// kill the source
			long pidToKill = 0;
			for (Map.Entry<Long, String> entry : mapPidUuid.entrySet()) {
				if (moduleContainers.getSourceContainer().equals(entry.getValue())) {
					pidToKill = entry.getKey();
					break;
				}
			}
			assertFalse(pidToKill == 0);
			logger.info("Killing container with pid {}", pidToKill);
			mapPidContainers.get(pidToKill).close();

			// ensure the module is picked up by another server
			RuntimeContainers redeployedModuleContainers = getRuntimeContainers(template);
			logger.debug("old source container:{}, new source container: {}",
					moduleContainers.getSourceContainer(), redeployedModuleContainers.getSourceContainer());
			assertNotEquals(moduleContainers.getSourceContainer(), redeployedModuleContainers.getSourceContainer());
		}
		finally {
			for (JavaApplication<SimpleJavaApplication> container : mapPidContainers.values()) {
				try {
					container.close();
				}
				catch (Exception e) {
					// ignore exceptions on shutdown
				}
			}
		}

	}

	private RuntimeContainers getRuntimeContainers(SpringXDTemplate template) throws InterruptedException {
		RuntimeContainers containers = new RuntimeContainers();
		long expiry = System.currentTimeMillis() + 10000;
		int moduleCount = 0;

		while (!containers.isComplete() && System.currentTimeMillis() < expiry) {
			Thread.sleep(500);
			moduleCount = 0;
			for (ModuleMetadataResource module : template.runtimeOperations().listRuntimeModules()) {
				String moduleId = module.getModuleId();
				if (moduleId.contains("source")) {
					containers.setSourceContainer(module.getContainerId());
				}
				else if (moduleId.contains("sink")) {
					containers.setSinkContainer(module.getContainerId());
				}
				else {
					throw new IllegalStateException(String.format(
							"Module '%s' is neither a source or sink", moduleId));
				}
				moduleCount++;
			}
		}
		assertTrue(containers.isComplete());
		assertEquals(2, moduleCount);

		return containers;
	}

	private class RuntimeContainers {
		private String sourceContainer;
		private String sinkContainer;

		public String getSourceContainer() {
			return sourceContainer;
		}

		public void setSourceContainer(String sourceContainer) {
			this.sourceContainer = sourceContainer;
		}

		public String getSinkContainer() {
			return sinkContainer;
		}

		public void setSinkContainer(String sinkContainer) {
			this.sinkContainer = sinkContainer;
		}

		public boolean isComplete() {
			return StringUtils.hasText(sourceContainer) && StringUtils.hasText(sinkContainer);
		}

	}

}
