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

package org.springframework.batch.integration.launch;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.test.support.JobSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;


@ContextConfiguration(locations = {
	"/org/springframework/batch/integration/launch/job-execution-context.xml" })
public class JobLaunchingMessageHandlerTests extends AbstractJUnit4SpringContextTests {

	JobLaunchRequestHandler messageHandler;

	StubJobLauncher jobLauncher;

	@Before
	public void setUp() {
		jobLauncher = new StubJobLauncher();
		messageHandler = new JobLaunchingMessageHandler(jobLauncher);
	}

	@Test
	public void testSimpleDelivery() throws Exception {
		messageHandler.launch(new JobLaunchRequest(new JobSupport("testjob"), null));
		assertEquals("Wrong job count", 1, jobLauncher.jobs.size());
		assertEquals("Wrong job name", jobLauncher.jobs.get(0).getName(), "testjob");
	}

	private static class StubJobLauncher implements JobLauncher {

		List<Job> jobs = new ArrayList<Job>();

		List<JobParameters> parameters = new ArrayList<JobParameters>();

		AtomicLong jobId = new AtomicLong();

		public JobExecution run(Job job, JobParameters jobParameters) {
			jobs.add(job);
			parameters.add(jobParameters);
			return new JobExecution(new JobInstance(jobId.getAndIncrement(), job.getName()), jobParameters);
		}
	}

}
