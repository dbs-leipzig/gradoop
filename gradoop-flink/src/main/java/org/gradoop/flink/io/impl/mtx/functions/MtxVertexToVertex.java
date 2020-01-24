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
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

import static org.gradoop.flink.io.impl.mtx.MtxDataSource.generateId;
import static org.gradoop.flink.io.impl.mtx.MtxDataSource.getSplitCharacter;
import static org.gradoop.flink.io.impl.mtx.MtxDataSource.isComment;

/**
 * Maps mtx-vertices to {@link EPGMVertex}
 */
public class MtxVertexToVertex implements FlatMapFunction<String, EPGMVertex> {
  /**
   * The EPGMVertexFactory<Vertex> to use for creating vertices
   */
  private VertexFactory<EPGMVertex> vertexFactory;
  /**
   * Is set to true once the first non-comment line is skipped.
   * This line contains the amount of vertices and edges and no 'real' data.
   */
  private boolean firstSkipped = false;
  /**
   * Create new VertexMapper
   *
   * @param vertexFactory The {@link VertexFactory} to use for creating vertices
   */
  public MtxVertexToVertex(VertexFactory<EPGMVertex> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public void flatMap(String line, Collector<EPGMVertex> collector) {
    if (!isComment(line)) {
      if (!firstSkipped) {
        firstSkipped = true;
        return;
      }
      String[] splitted = line.split(getSplitCharacter(line));
      collector.collect(vertexFactory.initVertex(generateId(splitted[0])));
      collector.collect(vertexFactory.initVertex(generateId(splitted[1])));
    }
  }
}
