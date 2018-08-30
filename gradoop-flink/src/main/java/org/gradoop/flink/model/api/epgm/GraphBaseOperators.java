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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Operators that are available at all graph structures.
 *
 * @see LogicalGraph
 * @see GraphCollection
 */
public interface GraphBaseOperators {

  /**
   * Returns the Gradoop Flink configuration.
   *
   * @return Gradoop Flink configuration
   */
  GradoopFlinkConfig getConfig();

  //----------------------------------------------------------------------------
  // Utility methods
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
   * Writes logical graph/graph collection to given data sink.
   *
   * @param dataSink data sink
   * @throws IOException if the data sink can't be written
   */
  void writeTo(DataSink dataSink) throws IOException;

  /**
   * Writes logical graph/graph collection to given data sink with overwrite option
   *
   * @param dataSink data sink
   * @param overWrite determines whether existing files are overwritten
   * @throws IOException if the data sink can't be written
   */
  void writeTo(DataSink dataSink, boolean overWrite) throws IOException;
}
