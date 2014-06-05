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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.hateoas.PagedResources;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.xd.rest.client.domain.ModuleMetadataResource;
import org.springframework.xd.rest.client.domain.StreamDefinitionResource;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;

/**
 * Multi container stream deployment tests.
 *
 * @author Patrick Peralta
 */
public class StreamDeploymentTest {

	/**
	 * Logger.
	 */
	private static final Logger logger = LoggerFactory.getLogger(StreamDeploymentTest.class);

	/**
	 * ZooKeeper server.
	 *
	 * @see DistributedTestUtils#startZooKeeper
	 */
	private static TestingServer zooKeeper;

	/**
	 * HSQL server.
	 *
	 * @see DistributedTestUtils#startHsql
	 */
	private static JavaApplication<SimpleJavaApplication> hsqlServer;

	/**
	 * Admin server.
	 *
	 * @see DistributedTestUtils#startAdmin
	 */
	private static JavaApplication<SimpleJavaApplication> adminServer;

	/**
	 * Name of currently executing unit test.
	 */
	@Rule
	public TestName testName = new TestName();


	/**
	 * Start the minimum required servers for a distributed XD system:
	 * <ul>
	 *     <li>ZooKeeper</li>
	 *     <li>HSQL</li>
	 *     <li>Admin server (for serving REST endpoints)</li>
	 * </ul>
	 *
	 * @throws Exception
	 */
	@BeforeClass
	public static void startServers() throws Exception {
		zooKeeper = startZooKeeper();
		hsqlServer = startHsql();
		adminServer = startAdmin();
	}

	/**
	 * Stop all of the servers started by {@link #startServers}
	 * after all testing is complete.
	 *
	 * @throws Exception
	 *
	 * @see #startServers
	 */
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

	/**
	 * Destroy all streams after each test.
	 *
	 * @throws Exception
	 */
	@After
	public void clearStreams() throws Exception {
		Assert.state(adminServer != null);
		SpringXDTemplate template = new SpringXDTemplate(new URI(ADMIN_URL));
		template.streamOperations().destroyAll();
	}

	/**
	 * Start three containers and deploy a simple two module stream.
	 * Kill the container hosting the source module and assert that
	 * the module is deployed to one of the remaining containers.
	 *
	 * @throws Exception
	 */
	@Test
	public void testKillOneContainer() throws Exception {
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

			String streamName = testName.getMethodName() + "-ticktock";

			template.streamOperations().createStream(streamName, "time|log", false);
			verifySingleStreamCreation(template, streamName);

			template.streamOperations().deploy(streamName, null);

			// verify modules
			ModuleRuntimeContainers moduleContainers = retrieveModuleRuntimeContainers(template);

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
			ModuleRuntimeContainers redeployedModuleContainers = retrieveModuleRuntimeContainers(template);
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

	/**
	 * Start two containers and deploy a simple two module stream.
	 * Shut down all of the containers. Start a new container and
	 * assert that the stream modules are deployed to the new container.
	 *
	 * @throws Exception
	 */
	@Test
	public void testKillAllContainers() throws Exception {
		Map<Long, JavaApplication<SimpleJavaApplication>> mapPidContainers = new HashMap<>();
		try {
			for (int i = 0; i < 2; i++) {
				JavaApplication<SimpleJavaApplication> containerServer = startContainer();
				mapPidContainers.put(containerServer.getId(), containerServer);
			}

			SpringXDTemplate template = new SpringXDTemplate(new URI(ADMIN_URL));
			logger.info("Waiting for containers...");
			waitForContainers(template, mapPidContainers.keySet());
			logger.info("Containers running");

			String streamName = testName.getMethodName() + "-ticktock";
			template.streamOperations().createStream(streamName, "time|log", true);

			// verify modules
			retrieveModuleRuntimeContainers(template);

			// kill all the containers
			for (JavaApplication<SimpleJavaApplication> container : mapPidContainers.values()) {
				container.close();
			}
			mapPidContainers.clear();
			Map<Long, String> map = waitForContainers(template, mapPidContainers.keySet());
			assertTrue(map.isEmpty());

			JavaApplication<SimpleJavaApplication> containerServer = startContainer();
			mapPidContainers.put(containerServer.getId(), containerServer);
			Map<Long, String> mapPidUuid = waitForContainers(template, mapPidContainers.keySet());
			assertEquals(1, mapPidUuid.size());
			String containerUuid = mapPidUuid.values().iterator().next();

			ModuleRuntimeContainers moduleContainers = retrieveModuleRuntimeContainers(template);
			assertEquals(containerUuid, moduleContainers.getSourceContainer());
			assertEquals(containerUuid, moduleContainers.getSinkContainer());
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

	/**
	 * Assert that:
	 * <ul>
	 *     <li>The given stream has been created</li>
	 *     <li>It is the only stream in the system</li>
	 * </ul>
	 * @param template    REST template for issuing admin commands
	 * @param streamName  name of stream to verify
	 */
	private void verifySingleStreamCreation(SpringXDTemplate template, String streamName) {
		PagedResources<StreamDefinitionResource> list = template.streamOperations().list();

		Iterator<StreamDefinitionResource> iterator = list.iterator();
		assertTrue(iterator.hasNext());

		StreamDefinitionResource stream = iterator.next();
		assertEquals(streamName, stream.getName());
		assertFalse(iterator.hasNext());
	}

	/**
	 * Block the executing thread until the Admin server reports exactly
	 * two runtime modules (a source and sink).
	 *
	 * @param template  REST template for issuing admin commands
	 * @return mapping of modules to the containers they are deployed to
	 * @throws InterruptedException
	 */
	private ModuleRuntimeContainers retrieveModuleRuntimeContainers(SpringXDTemplate template) throws InterruptedException {
		ModuleRuntimeContainers containers = new ModuleRuntimeContainers();
		long expiry = System.currentTimeMillis() + 30000;
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

	/**
	 * Mapping of source and sink modules to the containers they are
	 * deployed to.
	 */
	private class ModuleRuntimeContainers {
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

		/**
		 * Return true if a source and sink container have been
		 * populated.
		 *
		 * @return true if source and sink containers are non-null
		 */
		public boolean isComplete() {
			return StringUtils.hasText(sourceContainer) && StringUtils.hasText(sinkContainer);
		}

	}

}
