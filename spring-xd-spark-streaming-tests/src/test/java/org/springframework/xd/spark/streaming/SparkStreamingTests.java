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
package org.springframework.xd.spark.streaming;

import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;

import org.junit.Test;

import org.springframework.xd.shell.command.AbstractStreamIntegrationTest;
import org.springframework.xd.shell.command.fixtures.HttpSource;
import org.springframework.xd.shell.command.fixtures.XDMatchers;
import org.springframework.xd.test.fixtures.FileSink;

/**
 * @author Ilayaperumal Gopinathan
 */
public class SparkStreamingTests extends AbstractStreamIntegrationTest {

	static {
		// Change the transport here to run the test against other message bus transports.
		System.setProperty("XD_TRANSPORT", "local");
	}

	private static final String messageToPost = "foo foo foo";

	@Test
	public void testSparkProcessor() throws Exception {
		final HttpSource source = newHttpSource();
		final FileSink sink = newFileSink().binary(true);

		final String stream = String.format("%s | spark-word-count | %s", source, sink);
		stream().create(generateStreamName(), stream);
		source.ensureReady().postData(messageToPost);
		assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("(foo,3)"))));
	}

	@Test
	public void testSparkLog() throws Exception {
		String fileName = getClass().getSimpleName() + new Random().nextInt() + ".txt";
		File fileToDelete = new File(fileName);
		try {
			final HttpSource source = newHttpSource();
			final String stream = String.format("%s | spark-log --filePath=%s", source, fileName);
			stream().create(generateStreamName(), stream);
			source.ensureReady().postData(messageToPost);
			//explicit sleep to wait for the message to get processed.
			Thread.sleep(2000);
			FileReader reader = new FileReader(fileName);
			BufferedReader br = new BufferedReader(reader);
			String s;
			while (((s = br.readLine()) != null)) {
				// note: the written content will have timestamp before the message.
				assertTrue(s.endsWith(messageToPost));
			}
		}
		finally {
			if (fileToDelete.exists()) {
				fileToDelete.delete();
			}
		}
	}
}
