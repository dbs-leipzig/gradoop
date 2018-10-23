/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.functions.timeextractors;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;

/**
 * Map function to extract temporal information from an EPGM vertex to create a temporal vertex
 * with a time interval as validity.
 */
public interface VertexTimeIntervalExtractor extends TimeIntervalExtractor<Vertex, TemporalVertex> {

  @Override
  default TemporalVertex map(Vertex vertex) throws Exception {
    TemporalVertex temporalVertex = TemporalVertex.fromNonTemporalVertex(vertex);
    temporalVertex.setValidFrom(getValidFrom(vertex));
    temporalVertex.setValidTo(getValidTo(vertex));
    return temporalVertex;
  }
}
