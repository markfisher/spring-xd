package org.springframework.xd.spark.streaming;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.xd.shell.command.fixtures.XDMatchers.eventually;
import static org.springframework.xd.shell.command.fixtures.XDMatchers.fileContent;

import java.io.File;
import java.util.Random;

import org.junit.Test;

import org.springframework.xd.shell.command.AbstractStreamIntegrationTest;
import org.springframework.xd.shell.command.fixtures.HttpSource;
import org.springframework.xd.shell.command.fixtures.XDMatchers;
import org.springframework.xd.test.fixtures.FileSink;


/**
 * Abstract Spark streaming test class which can be extended to run against multiple XD transport.
 *
 * @author Ilayaperumal Gopinathan
 */
public abstract class AbstractSparkStreamingTests extends AbstractStreamIntegrationTest {

	private static final String TEST_MESSAGE = "foo foo foo";

	@Test
	public void testSparkProcessor() throws Exception {
		final HttpSource source = newHttpSource();
		final FileSink sink = newFileSink().binary(true);

		final String stream = String.format("%s | spark-word-count | %s", source, sink);
		stream().create(generateStreamName(), stream);
		source.ensureReady().postData(TEST_MESSAGE);
		assertThat(sink, XDMatchers.eventually(XDMatchers.hasContentsThat(equalTo("(foo,3)"))));
	}

	@Test
	public void testSparkLog() throws Exception {
		String fileName = getClass().getSimpleName() + new Random().nextInt() + ".txt";
		File file = new File(fileName);
		try {
			final HttpSource source = newHttpSource();
			final String stream = String.format("%s | spark-log --filePath=%s", source, fileName);
			stream().create(generateStreamName(), stream);
			source.ensureReady().postData(TEST_MESSAGE);
			// note: the written content will have timestamp before the message.
			assertThat(file, eventually(fileContent(endsWith(TEST_MESSAGE))));
		}
		finally {
			if (file.exists()) {
				file.delete();
			}
		}
	}

}
