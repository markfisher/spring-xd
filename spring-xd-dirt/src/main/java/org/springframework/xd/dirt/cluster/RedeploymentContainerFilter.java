/*
 * Copyright 2014-2015 the original author or authors.
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

package org.springframework.xd.dirt.cluster;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.util.Assert;
import org.springframework.xd.module.ModuleDescriptor;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * A {@link ContainerFilter} used for redeployment of modules when a container
 * exits the cluster. This filter provides the following functionality:
 * <ol>
 *     <li>only one container is returned</li>
 *     <li>containers whose names are present in the provided
 *         {@code exclusions} collection are not returned;
 *         these containers are presumed to have already
 *         deployed the module</li>
 * </ol>
 *
 * @author Patrick Peralta
 * @author Mark Fisher
 */
public class RedeploymentContainerFilter implements ContainerFilter {

	/**
	 * Set of container names that should <b>not</b> be returned by {@link #filterContainers}.
	 */
	private final Set<String> exclusions;

	/**
	 * Predicate used to determine if a container should be returned by {@link #filterContainers}.
	 */
	private final MatchingPredicate matchingPredicate;

	/**
	 * Construct a {@link RedeploymentContainerFilter} with the provided exclusions.
	 *
	 * @param exclusions        collection of container names to exclude
	 */
	public RedeploymentContainerFilter(Collection<String> exclusions) {
		Assert.notNull(exclusions);
		this.exclusions = Collections.unmodifiableSet(new HashSet<String>(exclusions));
		this.matchingPredicate = new MatchingPredicate();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<Container> filterContainers(ModuleDescriptor moduleDescriptor, Iterable<Container> containers) {
		Iterable<Container> matches = Iterables.filter(containers, matchingPredicate);
		return (matches.iterator().hasNext())
				? Collections.singleton(matches.iterator().next())
				: Collections.<Container>emptyList();
	}


	/**
	 * Predicate used to determine if a container should be returned by {@link #filterContainers}.
	 */
	private class MatchingPredicate implements Predicate<Container> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean apply(Container input) {
			return input != null && (!exclusions.contains(input.getName()));
		}
	}

}
