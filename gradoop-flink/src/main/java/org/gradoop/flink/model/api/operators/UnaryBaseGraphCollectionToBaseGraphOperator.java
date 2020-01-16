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
 * Creates a graph of type {@link LG} based on one input graph collection of the type {@link GC}.
 *
 * @param <GC> the type of the graph collection used as input value.
 * @param <LG> the type of the graph used as return value.
 */
public interface UnaryBaseGraphCollectionToBaseGraphOperator<
  GC extends BaseGraphCollection,
  LG extends BaseGraph> extends UnaryBaseGraphCollectionToValueOperator<GC, LG> {
}
