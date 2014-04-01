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

package org.springframework.xd.shell;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import org.springframework.integration.x.bus.MessageBus;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.CommandResult;
import org.springframework.shell.core.JLineShellComponent;
import org.springframework.util.AlternativeJdkIdGenerator;
import org.springframework.util.IdGenerator;
import org.springframework.xd.dirt.container.store.RuntimeContainerInfoRepository;
import org.springframework.xd.dirt.integration.test.SingleNodeIntegrationTestSupport;
import org.springframework.xd.dirt.module.ModuleDefinitionRepository;
import org.springframework.xd.dirt.server.SingleNodeApplication;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;
import org.springframework.xd.test.RandomConfigurationSupport;
import org.springframework.xd.test.redis.RedisTestSupport;

/**
 * Superclass for performing integration tests of spring-xd shell commands.
 * 
 * JUnit's BeforeClass and AfterClass annotations are used to start and stop the XDAdminServer in local mode with the
 * default store configured to use in-memory storage.
 * 
 * Note: This isn't ideal as it takes significant time to startup the embedded XDContainer/tomcat and we should do this
 * once across all tests.
 * 
 * @author Mark Pollack
 * @author Kashyap Parikh
 * @author David Turanski
 * 
 */
public abstract class AbstractShellIntegrationTest {

	/**
	 * Where test module definition assets reside, relative to this project cwd.
	 */
	private static final File TEST_MODULES_SOURCE = new File("src/test/resources/spring-xd/xd/modules/");

	/**
	 * Where test modules should end up, relative to this project cwd.
	 */
	private static final File TEST_MODULES_TARGET = new File("../modules/");

	protected static final String DEFAULT_METRIC_NAME = "bar";

	public static boolean SHUTDOWN_AFTER_RUN = true;

	private final IdGenerator idGenerator = new AlternativeJdkIdGenerator();

	@ClassRule
	public static RedisTestSupport redisAvailableRule = new RedisTestSupport();

	private static final Log logger = LogFactory.getLog(AbstractShellIntegrationTest.class);

	private static SingleNodeApplication application;

	private static JLineShellComponent shell;

	private Set<File> toBeDeleted = new HashSet<File>();

	private static RuntimeContainerInfoRepository runtimeInformationRepository;

	protected static StreamCommandListener streamCommandListener;

	protected static JobCommandListener jobCommandListener = new JobCommandListener();

	private static SingleNodeIntegrationTestSupport integrationTestSupport;

	@BeforeClass
	public static synchronized void startUp() throws InterruptedException, IOException {
		RandomConfigurationSupport randomConfigSupport = new RandomConfigurationSupport();
		if (application == null) {
			application = new SingleNodeApplication().run("--transport", "local",
					"--analytics", "redis",
					"--store", "redis"
					);
			integrationTestSupport = new SingleNodeIntegrationTestSupport(application);

			streamCommandListener = new StreamCommandListener(
					integrationTestSupport.streamDefinitionRepository(),
					application.containerContext().getBean(ModuleDefinitionRepository.class),
					application.containerContext().getBean(ModuleOptionsMetadataResolver.class));

			integrationTestSupport.addPathListener(Paths.STREAMS, streamCommandListener);
			integrationTestSupport.addPathListener(Paths.JOBS, jobCommandListener);

			Bootstrap bootstrap = new Bootstrap(new String[] { "--port", randomConfigSupport.getAdminServerPort() });
			shell = bootstrap.getJLineShellComponent();

			runtimeInformationRepository = application.pluginContext().getBean(
					RuntimeContainerInfoRepository.class);
		}
		if (!shell.isRunning()) {
			shell.start();
		}
	}

	@AfterClass
	public static void shutdown() {
		if (SHUTDOWN_AFTER_RUN) {
			runtimeInformationRepository.delete(application.pluginContext().getId());
			logger.info("Stopping XD Shell");
			shell.stop();
			if (application != null) {
				logger.info("Stopping Single Node Server");
				application.close();
				redisAvailableRule.getResource().destroy();
			}
		}
	}

	public static JLineShellComponent getShell() {
		return shell;
	}

	protected MessageBus getMessageBus() {
		return integrationTestSupport.messageBus();
	}

	private String generateUniqueName(String name) {
		return name + "-" + idGenerator.generateId();
	}

	private String generateUniqueName() {
		StackTraceElement[] element = Thread.currentThread().getStackTrace();
		// Assumption here is that the generateStreamName()/generateJobName() is called from the @Test method
		return generateUniqueName(element[4].getMethodName());
	}

	protected String generateStreamName(String name) {
		return (name == null) ? generateUniqueName() : generateUniqueName(name);
	}

	protected String generateStreamName() {
		return generateStreamName(null);
	}

	protected String getTapName(String streamName) {
		return "tap:stream:" + streamName;
	}

	protected String generateJobName(String name) {
		return (name == null) ? generateUniqueName() : generateUniqueName(name);
	}

	protected String generateJobName() {
		return generateJobName(null);
	}

	protected String getJobLaunchQueue(String jobName) {
		return "queue:job:" + jobName;
	}

	/**
	 * Execute a command and verify the command result.
	 */
	protected CommandResult executeCommand(String command) {
		CommandResult cr = getShell().executeCommand(command);
		assertTrue("Failure.  CommandResult = " + cr.toString(), cr.isSuccess());
		return cr;
	}

	protected CommandResult executeCommandExpectingFailure(String command) {
		CommandResult cr = getShell().executeCommand(command);
		assertFalse("Expected command to fail.  CommandResult = " + cr.toString(), cr.isSuccess());
		return cr;
	}

	/**
	 * Copies over module files (including jars if this is a directory-style module) from src/test/resources to where it
	 * will be picked up and makes sure it will disappear at test end.
	 * 
	 * @param type the type of module, e.g. "source"
	 * @param name the module name, with extension (e.g. time2.xml or time2 if a directory)
	 * @throws IOException
	 */
	protected void installTestModule(String type, String name) throws IOException {
		File toCopy = new File(TEST_MODULES_SOURCE, type + File.separator + name);
		File destination = new File(TEST_MODULES_TARGET, type + File.separator + name);
		Assert.assertFalse(
				String.format("Destination %s already present. Make sure you're not overwriting a "
						+ "standard module, or if this is from a previous aborted test run, please delete manually",
						destination),
				destination.exists());
		toBeDeleted.add(destination);
		if (toCopy.isDirectory()) {
			FileUtils.copyDirectory(toCopy, destination);
		}
		else {
			FileUtils.copyFile(toCopy, destination);
		}
	}

	@After
	public void cleanTestModuleFiles() {
		for (File file : toBeDeleted) {
			FileUtils.deleteQuietly(file);
		}
	}

}
