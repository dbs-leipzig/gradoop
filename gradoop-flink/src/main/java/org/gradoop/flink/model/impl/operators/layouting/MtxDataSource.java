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
package org.gradoop.flink.model.impl.operators.layouting;

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
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * An importer for the matrix-marked format (.mtx)
 * https://math.nist.gov/MatrixMarket/formats.html
 */
public class MtxDataSource implements DataSource {

  /** Path to the input-file */
  private String path;
  /** Gradoop config */
  private GradoopFlinkConfig cfg;
  /** if true, skip the pre-processing (remove multi-edges, self-edges etc.) */
  private boolean skipPreprocessing;

  /**
   * Create new MTX-Datasource
   * @param path Pathh to the input-file
   * @param cfg Gradoop-config to use
   */
  public MtxDataSource(String path, GradoopFlinkConfig cfg) {
    this(path, cfg, false);
  }

  /**
   * Create new MTX-Datasource
   * @param path Pathh to the input-file
   * @param cfg Gradoop-config to use
   * @param skipPreprocessing if true, skip the pre-processing (remove multi-edges, self-edges etc.)
   */
  public MtxDataSource(String path, GradoopFlinkConfig cfg, boolean skipPreprocessing) {
    this.path = path;
    this.cfg = cfg;
    this.skipPreprocessing = skipPreprocessing;
  }

  @Override
  public LogicalGraph getLogicalGraph() {
    DataSet<EPGMVertex> vertices = cfg.getExecutionEnvironment().readTextFile(path)
      .flatMap(new VertexMapper(cfg.getLogicalGraphFactory().getVertexFactory())).distinct("id");

    DataSet<EPGMEdge> edges = cfg.getExecutionEnvironment().readTextFile(path)
      .flatMap(new EdgeMapper(cfg.getLogicalGraphFactory().getEdgeFactory()));

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
   * @param line The line to check
   * @return true if comment
   */
  private static boolean isComment(String line) {
    return line.startsWith("#") || line.startsWith("%");
  }

  /**
   * Get the character that should be used to split the given line
   * @param text The line to check
   * @return A string containing either a space or tab character
   */
  private static String getSplitCharacter(String text) {
    return text.contains(" ") ? " " : "\t";
  }

  /**
   * Generate a GradoopId from an mtx-id
   * @param text The numerical id of the vertex from the mtx file
   * @return A GradooopId for the vertex
   */
  private static GradoopId generateId(String text) {
    String hex =
      String.format("%24s", Integer.toHexString(Integer.parseInt(text))).replace(' ', '0');
    return GradoopId.fromString(hex);
  }

  /**
   * Maps mtx-edges to gradoop edges
   */
  private static class EdgeMapper implements FlatMapFunction<String, EPGMEdge> {
    /** The EPGMEdgeFactory<Edge> to use for creating Edges */
    private EdgeFactory<EPGMEdge> edgeFactory;

    /**
     * Create new EdgeMapper
     * @param edgeFactory The EPGMEdgeFactory<Edge> to use for creating Edges
     */
    EdgeMapper(EdgeFactory<EPGMEdge> edgeFactory) {
      this.edgeFactory = edgeFactory;
    }

    @Override
    public void flatMap(String line, Collector<EPGMEdge> collector) {
      if (!isComment(line)) {
        String[] splitted = line.split(getSplitCharacter(line));
        collector.collect(edgeFactory
          .initEdge(GradoopId.get(), "edge", generateId(splitted[0]), generateId(splitted[1])));
      }
    }
  }

  /**
   * Maps mtx-vertices to vertices
   */
  private static class VertexMapper implements FlatMapFunction<String, EPGMVertex> {
    /** The EPGMVertexFactory<Vertex> to use for creating vertices */
    private VertexFactory<EPGMVertex> vertexFactory;

    /**
     * Create new VertexMapper
     * @param vertexFactory The EPGMVertexFactory<Vertex> to use for creating vertices
     */
    VertexMapper(VertexFactory<EPGMVertex> vertexFactory) {
      this.vertexFactory = vertexFactory;
    }

    @Override
    public void flatMap(String line, Collector<EPGMVertex> collector) {
      if (!isComment(line)) {
        String[] splitted = line.split(getSplitCharacter(line));
        collector.collect(vertexFactory.initVertex(generateId(splitted[0]), "vertex"));
        collector.collect(vertexFactory.initVertex(generateId(splitted[1]), "vertex"));
      }
    }
  }
}
