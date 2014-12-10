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
package org.springframework.xd.module.spark.examples;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.springframework.xd.module.spark.SparkModule;

/**
 * @author Mark Fisher
 */
@SuppressWarnings({"unchecked","rawtypes","serial"})
public class SparkLogModule implements SparkModule {

	private static final SimpleDateFormat sdf = new SimpleDateFormat("[hh:mm:ss]");

	public JavaDStreamLike process(JavaDStreamLike input) {
		input.foreachRDD(new Function<JavaRDD, Void>() {
			public Void call(JavaRDD rdd) {
				rdd.foreachPartition(new VoidFunction<Iterator<?>>() {
					public void call(Iterator<?> items) throws Exception {
						final StringBuffer buffer = new StringBuffer(sdf.format(new Date()));
						while (items.hasNext()) {
							buffer.append(items.next());
						}
						System.out.println(buffer.toString());
					}
				});
				return null;
			}
		});
		return null;
	}

}
