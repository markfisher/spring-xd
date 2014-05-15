/*
 * Copyright 2013-2014 the original author or authors.
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

package org.springframework.xd.dirt.plugins;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.module.ModuleType;
import org.springframework.xd.module.core.Module;


/**
 * Abstract class that extends {@link AbstractMessageBusBinderPlugin} and has common implementation methods related to
 * stream plugins.
 *
 * @author Ilayaperumal Gopinathan
 * @author Mark Fisher
 */
public abstract class AbstractStreamPlugin extends AbstractMessageBusBinderPlugin {

	public AbstractStreamPlugin(MessageBus messageBus) {
		super(messageBus);
	}

	@Override
	protected String getInputChannelName(Module module) {
		ModuleDescriptor descriptor = module.getDescriptor();
		String sourceChannel = descriptor.getSourceChannelName();
		return (sourceChannel != null) ? sourceChannel : descriptor.getGroup() + "." + (descriptor.getIndex() - 1);
	}

	@Override
	protected String getOutputChannelName(Module module) {
		ModuleDescriptor descriptor = module.getDescriptor();
		String sinkChannel = descriptor.getSinkChannelName();
		return (sinkChannel != null) ? sinkChannel : descriptor.getGroup() + "." + descriptor.getIndex();
	}

	@Override
	protected String buildTapChannelName(Module module) {
		Assert.isTrue(module.getType() != ModuleType.job, "Job module type not supported.");
		ModuleDescriptor descriptor = module.getDescriptor();
		// for Stream return channel name with indexed elements
		return String.format("%s%s%s.%s.%s", TAP_CHANNEL_PREFIX, "stream:", descriptor.getGroup(),
				module.getName(), descriptor.getIndex());
	}

	@Override
	public boolean supports(Module module) {
		ModuleType moduleType = module.getType();
		return (moduleType == ModuleType.source || moduleType == ModuleType.processor || moduleType == ModuleType.sink);
	}
}
