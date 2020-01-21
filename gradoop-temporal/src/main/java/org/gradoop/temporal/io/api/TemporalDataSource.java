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
package org.gradoop.temporal.io.api;

import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;

import java.io.IOException;

/**
 * A data source for temporal graphs and graph collections.
 */
public interface TemporalDataSource {

  /**
   * Reads the input as temporal graph.
   *
   * @return temporal graph
   * @throws IOException if the reading of the graph data fails
   */
  TemporalGraph getTemporalGraph() throws IOException;

  /**
   * Reads the input as temporal graph collection.
   *
   * @return temporal graph collection
   * @throws IOException if the reading of the graph data fails
   */
  TemporalGraphCollection getTemporalGraphCollection() throws IOException;
}
