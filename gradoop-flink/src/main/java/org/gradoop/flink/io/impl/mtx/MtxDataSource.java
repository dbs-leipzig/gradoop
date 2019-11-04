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
package org.gradoop.flink.io.impl.mtx;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSource;
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
  private GradoopFlinkConfig cfg;
  /**
   * if true, skip the pre-processing (remove multi-edges, self-edges etc.)
   */
  private boolean skipPreprocessing;

  /**
   * Create new MTX-Datasource
   *
   * @param path Path to the input-file
   * @param cfg  {@link GradoopFlinkConfig} to use
   */
  public MtxDataSource(String path, GradoopFlinkConfig cfg) {
    this(path, cfg, false);
  }

  /**
   * Create new MTX-Datasource
   *
   * @param path              Path to the input-file
   * @param cfg               {@link GradoopFlinkConfig} to use
   * @param skipPreprocessing if true, skip the pre-processing (remove multi-edges, self-edges etc.)
   */
  public MtxDataSource(String path, GradoopFlinkConfig cfg, boolean skipPreprocessing) {
    if (path == null || cfg == null) {
      throw new IllegalArgumentException("Arguments can not be null");
    }
    this.path = path;
    this.cfg = cfg;
    this.skipPreprocessing = skipPreprocessing;
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    DataSet<EPGMVertex> vertices = cfg.getExecutionEnvironment().readTextFile(path)
      .flatMap(new MtxVertexToVertex(cfg.getLogicalGraphFactory().getVertexFactory()))
      .distinct(new Id<>());

    DataSet<EPGMEdge> edges = cfg.getExecutionEnvironment().readTextFile(path)
      .flatMap(new MtxEdgeToEdge(cfg.getLogicalGraphFactory().getEdgeFactory()));

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

    return cfg.getLogicalGraphFactory().fromDataSets(vertices, edges);
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
  private static boolean isComment(String line) {
    return line.startsWith("%");
  }

  /**
   * Get the character that should be used to split the given line
   *
   * @param text The line to check
   * @return A string containing either a space or tab character
   */
  private static String getSplitCharacter(String text) {
    return text.contains(" ") ? " " : "\t";
  }

  /**
   * Generate a {@link GradoopId} from an mtx-id
   *
   * @param text The numerical id of the vertex from the mtx file
   * @return A {@link GradoopId} for the vertex
   */
  private static GradoopId generateId(String text) {
    String hex =
      String.format("%24s", Integer.toHexString(Integer.parseInt(text))).replace(' ', '0');
    return GradoopId.fromString(hex);
  }

  /**
   * Maps mtx-edges to {@link EPGMEdge}
   */
  private static class MtxEdgeToEdge implements FlatMapFunction<String, EPGMEdge> {
    /**
     * The EPGMEdgeFactory<Edge> to use for creating edges
     */
    private EdgeFactory<EPGMEdge> edgeFactory;

    /**
     * Create new EdgeMapper
     *
     * @param edgeFactory The {@link EdgeFactory} to use for creating Edges
     */
    MtxEdgeToEdge(EdgeFactory<EPGMEdge> edgeFactory) {
      this.edgeFactory = edgeFactory;
    }

    @Override
    public void flatMap(String line, Collector<EPGMEdge> collector) {
      if (!isComment(line)) {
        String[] splitted = line.split(getSplitCharacter(line));
        collector.collect(edgeFactory
          .initEdge(GradoopId.get(), generateId(splitted[0]), generateId(splitted[1])));
      }
    }
  }

  /**
   * Maps mtx-vertices to {@link EPGMVertex}
   */
  private static class MtxVertexToVertex implements FlatMapFunction<String, EPGMVertex> {
    /**
     * The EPGMVertexFactory<Vertex> to use for creating vertices
     */
    private VertexFactory<EPGMVertex> vertexFactory;

    /**
     * Create new VertexMapper
     *
     * @param vertexFactory The {@link VertexFactory} to use for creating vertices
     */
    MtxVertexToVertex(VertexFactory<EPGMVertex> vertexFactory) {
      this.vertexFactory = vertexFactory;
    }

    @Override
    public void flatMap(String line, Collector<EPGMVertex> collector) {
      if (!isComment(line)) {
        String[] splitted = line.split(getSplitCharacter(line));
        collector.collect(vertexFactory.initVertex(generateId(splitted[0])));
        collector.collect(vertexFactory.initVertex(generateId(splitted[1])));
      }
    }
  }
}
