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
package org.gradoop.flink.io.api;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.io.IOException;

/**
 * Data source in analytical programs.
 */
public interface DataSource
  extends BaseDataSource<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection> {

  /**
   * Reads the input as logical graph.
   *
   * @return logical graph
   * @throws IOException on failure
   */
  LogicalGraph getLogicalGraph() throws IOException;

  /**
   * Reads the input as graph collection.
   *
   * @return graph collection
   * @throws IOException on failure
   */
  GraphCollection getGraphCollection() throws IOException;
}
