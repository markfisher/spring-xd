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

package org.springframework.xd.dirt.server;

/**
 * The kind of transport used for communication between the admin server and container(s).
 *
 * @author Eric Bottard
 */
public enum Transport {

	/**
	 * Use same-process communication, using an in memory queue.
	 */
	local,

	/**
	 * Use redis (http://redis.io) as the communication middleware.
	 */
	redis,

//	/**
//	 * Use RabbitMQ (http://www.rabbitmq.com/) as the communication middleware.
//	 */
//	rabbitmq
	;

}
