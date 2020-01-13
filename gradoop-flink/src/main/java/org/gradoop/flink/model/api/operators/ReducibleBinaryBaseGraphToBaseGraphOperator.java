/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.api.operators;

import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;

/**
 * A marker interface for instances of {@link BinaryBaseGraphToBaseGraphOperator} that
 * support the reduction of a collection to a single base graph.
 *
 * @param <GC> the type of the graph collection used as input.
 * @param <LG> the type of the graph used as return value.
 */
public interface ReducibleBinaryBaseGraphToBaseGraphOperator<
  GC extends BaseGraphCollection,
  LG extends BaseGraph> extends UnaryBaseGraphCollectionToBaseGraphOperator<GC, LG> {
}
