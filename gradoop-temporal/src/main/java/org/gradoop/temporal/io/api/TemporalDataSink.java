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
 * A data sink for temporal graphs and collections.
 */
public interface TemporalDataSink {

  /**
   * Writes a temporal graph to the data sink.
   *
   * @param temporalGraph temporal graph
   * @throws IOException if the writing of the graph data fails
   */
  void write(TemporalGraph temporalGraph) throws IOException;

  /**
   * Writes a temporal graph collection to the data sink.
   *
   * @param temporalGraphCollection temporal graph collection
   * @throws IOException if the writing of the graph data fails
   */
  void write(TemporalGraphCollection temporalGraphCollection) throws IOException;

  /**
   * Writes a temporal graph to the data sink.
   *
   * @param temporalGraph temporal graph
   * @param overwrite true, if existing files should be overwritten
   * @throws IOException if the writing of the graph data fails
   */
  void write(TemporalGraph temporalGraph, boolean overwrite) throws IOException;

  /**
   * Writes a temporal graph collection to the data sink.
   *
   * @param temporalGraphCollection temporal graph collection
   * @param overwrite true, if existing files should be overwritten
   * @throws IOException if the writing of the graph data fails
   */
  void write(TemporalGraphCollection temporalGraphCollection, boolean overwrite) throws IOException;
}
