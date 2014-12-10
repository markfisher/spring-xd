/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.xd.shell.command;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.xd.shell.command.fixtures.XDMatchers.*;

import org.junit.Test;

import org.springframework.xd.shell.command.fixtures.HttpSource;
import org.springframework.xd.test.fixtures.FileSink;

/**
 * @author Ilayaperumal Gopinathan
 */
public class SparkStreamingTests extends AbstractStreamIntegrationTest {

	@Test
	public void testSparkProcessor() throws Exception {
		System.setProperty("spark.master.url", "local[5]");

		//final FileSource source = newFileSource();
		final HttpSource source = newHttpSource();
		final FileSink sink = newFileSink().binary(true);

		//source.appendToFile("foo foo foo");

		final String stream = String.format("f1: %s | spark-word-count | f2: %s", source, sink);
		stream().create(generateStreamName(), stream);
		source.ensureReady().postData("foo foo foo");
		//Thread.sleep(10000);
		assertThat(sink, eventually(hasContentsThat(equalTo("(foo,3)"))));
	}


}
