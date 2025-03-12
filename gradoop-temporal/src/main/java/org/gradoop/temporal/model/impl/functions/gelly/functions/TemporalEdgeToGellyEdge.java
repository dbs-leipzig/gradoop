/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.functions.gelly.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Convert input to a Gelly Edge with K key and EV value.
 * @param <E> Input type of temporal edge
 * @param <EV>  Value type of the output gelly edge.
 */
public interface TemporalEdgeToGellyEdge<E extends TemporalEdge, EV> extends TemporalElementToGellyEdge<E, GradoopId, EV> {
}
