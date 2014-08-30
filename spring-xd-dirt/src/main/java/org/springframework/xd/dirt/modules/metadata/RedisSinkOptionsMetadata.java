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

package org.springframework.xd.dirt.modules.metadata;

import static org.springframework.xd.module.options.spi.ModulePlaceholders.XD_STREAM_NAME;

import org.hibernate.validator.constraints.NotBlank;

import org.springframework.xd.module.options.spi.ModuleOption;

/**
 * Options for the 'redis' sink module.
 *
 * @author Mark Fisher
 */
public class RedisSinkOptionsMetadata {

	private String collection = "xd.sink." + XD_STREAM_NAME;

	@NotBlank
	public String getCollection() {
		return collection;
	}

	@ModuleOption("collection name to use")
	public void setCollection(String collection) {
		this.collection = collection;
	}

}
