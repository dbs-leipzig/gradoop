/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.operators.BaseGraphCollectionOperatorSupport;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.io.IOException;

/**
 * Defines the operators that are available on a {@link GraphCollection}.
 */
public interface GraphCollectionOperators extends BaseGraphCollectionOperatorSupport<LogicalGraph, GraphCollection> {

  //----------------------------------------------------------------------------
  // Auxiliary operators
  //----------------------------------------------------------------------------

  /**
   * Returns a 1-element dataset containing a {@code boolean} value which
   * indicates if the collection is empty.
   *
   * A collection is considered empty, if it contains no logical graphs.
   *
   * @return  1-element dataset containing {@code true}, if the collection is
   *          empty or {@code false} if not
   */
  DataSet<Boolean> isEmpty();

  /**
     * Writes the graph collection to the given data sink.
     *
     * @param dataSink The data sink to which the graph collection should be written.
     * @throws IOException if the collection can't be written to the sink
     */
  void writeTo(DataSink dataSink) throws IOException;

  /**
     * Writes the graph collection to the given data sink with an optional overwrite option.
     *
     * @param dataSink The data sink to which the graph collection should be written.
     * @param overWrite determines whether existing files are overwritten
     * @throws IOException if the collection can't be written to the sink
     */
  void writeTo(DataSink dataSink, boolean overWrite) throws IOException;
}
