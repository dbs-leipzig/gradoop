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

import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.operators.difference.Difference;
import org.gradoop.flink.model.impl.operators.intersection.Intersection;
import org.gradoop.flink.model.impl.operators.union.Union;

/**
 * Creates a {@link GraphCollection} based on two input collections.
 *
 * @see Union
 * @see Intersection
 * @see Difference
 *
 * @param <GC> the type of the graph collection used as input and return value.
 */
public interface BinaryBaseGraphCollectionToBaseGraphCollectionOperator<GC extends BaseGraphCollection>
  extends BinaryBaseGraphCollectionToValueOperator<GC, GC> {
}
