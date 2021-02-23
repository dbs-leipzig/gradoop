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
package org.gradoop.flink.io.impl.mtx;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.mtx.functions.MtxEdgeToEdge;
import org.gradoop.flink.io.impl.mtx.functions.MtxVertexToVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * An importer for the
 * <a href="https://math.nist.gov/MatrixMarket/formats.html">matrix market format</a> (.mtx)
 */
public class MtxDataSource implements DataSource {

  /**
   * Path to the input-file
   */
  private String path;
  /**
   * Gradoop config
   */
  private GradoopFlinkConfig config;
  /**
   * if true, skip the pre-processing (remove multi-edges, self-edges etc.)
   */
  private boolean skipPreprocessing;

  /**
   * Create new MTX-Datasource
   *
   * @param path Path to the input-file
   * @param config  {@link GradoopFlinkConfig} to use
   */
  public MtxDataSource(String path, GradoopFlinkConfig config) {
    this(path, config, false);
  }

  /**
   * Create new MTX-Datasource
   *
   * @param path              Path to the input-file
   * @param config               {@link GradoopFlinkConfig} to use
   * @param skipPreprocessing if true, skip the pre-processing (remove multi-edges, self-edges etc.)
   */
  public MtxDataSource(String path, GradoopFlinkConfig config, boolean skipPreprocessing) {
    if (path == null || config == null) {
      throw new IllegalArgumentException("Arguments can not be null");
    }
    this.path = path;
    this.config = config;
    this.skipPreprocessing = skipPreprocessing;
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    DataSet<EPGMVertex> vertices = config.getExecutionEnvironment().readTextFile(path)
      .setParallelism(1)
      .flatMap(new MtxVertexToVertex(config.getLogicalGraphFactory().getVertexFactory()))
      .setParallelism(1)
      .distinct(new Id<>());

    DataSet<EPGMEdge> edges = config.getExecutionEnvironment().readTextFile(path).setParallelism(1)
      .flatMap(new MtxEdgeToEdge(config.getLogicalGraphFactory().getEdgeFactory()))
      .setParallelism(1);

    if (!skipPreprocessing) {
      edges = edges.filter((e) -> !e.getSourceId().equals(e.getTargetId()));
      edges = edges.map(e -> {
        if (e.getSourceId().compareTo(e.getTargetId()) > 0) {
          GradoopId old = e.getSourceId();
          e.setSourceId(e.getTargetId());
          e.setTargetId(old);
        }
        return e;
      }).distinct("sourceId", "targetId");
    }

    return config.getLogicalGraphFactory().fromDataSets(vertices, edges);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    throw new UnsupportedOperationException("MTX does not support graph collections");
  }

  /**
   * Returns true if the given line is a comment
   *
   * @param line The line to check
   * @return true if comment
   */
  public static boolean isComment(String line) {
    return line.startsWith("%");
  }

  /**
   * Get the character that should be used to split the given line
   *
   * @param text The line to check
   * @return A string containing either a space or tab character
   */
  public static String getSplitCharacter(String text) {
    return text.contains(" ") ? " " : "\t";
  }

  /**
   * Generate a {@link GradoopId} from an mtx-id
   *
   * @param text The numerical id of the vertex from the mtx file
   * @return A {@link GradoopId} for the vertex
   */
  public static GradoopId generateId(String text) {
    String hex =
      String.format("%24s", Integer.toHexString(Integer.parseInt(text))).replace(' ', '0');
    return GradoopId.fromString(hex);
  }
}
