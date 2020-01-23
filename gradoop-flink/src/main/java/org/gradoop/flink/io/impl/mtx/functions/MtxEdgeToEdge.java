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
package org.gradoop.flink.io.impl.mtx.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

import static org.gradoop.flink.io.impl.mtx.MtxDataSource.generateId;
import static org.gradoop.flink.io.impl.mtx.MtxDataSource.getSplitCharacter;
import static org.gradoop.flink.io.impl.mtx.MtxDataSource.isComment;

/**
 * Maps mtx-edges to {@link EPGMEdge}
 */
public class MtxEdgeToEdge implements FlatMapFunction<String, EPGMEdge> {
  /**
   * The EPGMEdgeFactory<Edge> to use for creating edges
   */
  private EdgeFactory<EPGMEdge> edgeFactory;
  /**
   * Is set to true once the first non-comment line is skipped.
   * This line contains the amount of vertices and edges and no 'real' data.
   */
  private boolean firstSkipped = false;

  /**
   * Create new EdgeMapper
   *
   * @param edgeFactory The {@link EdgeFactory} to use for creating Edges
   */
  public MtxEdgeToEdge(EdgeFactory<EPGMEdge> edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void flatMap(String line, Collector<EPGMEdge> collector) {
    if (!isComment(line)) {
      if (!firstSkipped) {
        firstSkipped = true;
        return;
      }
      String[] splitted = line.split(getSplitCharacter(line));
      collector.collect(edgeFactory
        .initEdge(GradoopId.get(), generateId(splitted[0]), generateId(splitted[1])));
    }
  }
}
