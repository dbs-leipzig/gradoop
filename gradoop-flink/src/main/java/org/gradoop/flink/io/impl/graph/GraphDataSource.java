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
package org.gradoop.flink.io.impl.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.functions.InitEdge;
import org.gradoop.flink.io.impl.graph.functions.InitVertex;
import org.gradoop.flink.io.impl.graph.functions.UpdateEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tuple.Project3To0And1;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transforms an external graph into an EPGM database. The external graph needs
 * to be represented by a data set of {@link ImportVertex} and a data set of
 * {@link ImportEdge}. This class transforms the external graph into an EPGM
 * {@link LogicalGraph}.
 *
 * @param <K> External vertex/edge identifier type
 */
public class GraphDataSource<K extends Comparable<K>> implements DataSource {

  /**
   * Vertices to import.
   */
  private final DataSet<ImportVertex<K>> importVertices;

  /**
   * Edges to import.
   */
  private final DataSet<ImportEdge<K>> importEdges;

  /**
   * Used to store the lineage information at the resulting EPGM element.
   */
  private final String lineagePropertyKey;

  /**
   * Gradoop config
   */
  private final GradoopFlinkConfig config;


  /**
   * Creates a new graph reader with no lineage information stored at the
   * resulting EPGM graph.
   *
   * @param importVertices  vertices to import
   * @param importEdges     edges to import
   * @param config          gradoop config
   */
  public GraphDataSource(DataSet<ImportVertex<K>> importVertices,
    DataSet<ImportEdge<K>> importEdges, GradoopFlinkConfig config) {
    this(importVertices, importEdges, null, config);
  }

  /**
   * Creates a new graph reader.
   *
   * If the given {@code lineagePropertyKey} is {@code null}, no lineage info
   * is stored.
   *
   * @param importVertices      vertices to import
   * @param importEdges         edges to import
   * @param lineagePropertyKey  property key to store import identifiers
   *                            (can be {@code null})
   * @param config              gradoop config
   */
  public GraphDataSource(DataSet<ImportVertex<K>> importVertices,
    DataSet<ImportEdge<K>> importEdges, String lineagePropertyKey,
    GradoopFlinkConfig config) {
    this.importVertices     = checkNotNull(importVertices);
    this.importEdges        = checkNotNull(importEdges);
    this.lineagePropertyKey = lineagePropertyKey;
    this.config             = checkNotNull(config);
  }

  /**
   * Transforms the import graph into a logical graph.
   *
   * @return logical graph
   */
  @Override
  public LogicalGraph getLogicalGraph() {

    TypeInformation<K> externalIdType = ((TupleTypeInfo<?>) importVertices
      .getType()).getTypeAt(0);

    DataSet<Tuple3<K, GradoopId, Vertex>> vertexTriples = importVertices
      .map(new InitVertex<K>(
        config.getVertexFactory(), lineagePropertyKey, externalIdType));

    DataSet<Vertex> epgmVertices = vertexTriples
      .map(new Value2Of3<K, GradoopId, Vertex>());

    DataSet<Tuple2<K, GradoopId>> vertexIdPair = vertexTriples
      .map(new Project3To0And1<K, GradoopId, Vertex>());

    DataSet<Edge> epgmEdges = importEdges
      .join(vertexIdPair)
      .where(1).equalTo(0)
      .with(new InitEdge<K>(
        config.getEdgeFactory(), lineagePropertyKey, externalIdType))
      .join(vertexIdPair)
      .where(0).equalTo(0)
      .with(new UpdateEdge<Edge, K>());

    return config.getLogicalGraphFactory().fromDataSets(epgmVertices, epgmEdges);
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }
}
