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
package org.gradoop.flink.model.impl.operators.tostring;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryGraphCollectionToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelCombiner;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.flink.model.impl.operators.tostring.functions.AdjacencyMatrix;
import org.gradoop.flink.model.impl.operators.tostring.functions.ConcatGraphHeadStrings;
import org.gradoop.flink.model.impl.operators.tostring.functions.IncomingAdjacencyList;
import org.gradoop.flink.model.impl.operators.tostring.functions.MultiEdgeStringCombiner;
import org.gradoop.flink.model.impl.operators.tostring.functions.OutgoingAdjacencyList;
import org.gradoop.flink.model.impl.operators.tostring.functions.SourceStringUpdater;
import org.gradoop.flink.model.impl.operators.tostring.functions.SwitchSourceTargetIds;
import org.gradoop.flink.model.impl.operators.tostring.functions.TargetStringUpdater;
import org.gradoop.flink.model.impl.operators.tostring.functions.UndirectedAdjacencyList;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * Operator deriving a string representation from a graph collection.
 * The representation follows the concept of a canonical adjacency matrix.
 */
public class CanonicalAdjacencyMatrixBuilder implements
  UnaryGraphCollectionToValueOperator<String> {

  /**
   * function describing string representation of graph heads
   */
  private final GraphHeadToString<GraphHead> graphHeadToString;
  /**
   * function describing string representation of vertices
   */
  private final VertexToString<Vertex> vertexToString;
  /**
   * function describing string representation of edges
   */
  private final EdgeToString<Edge> egeLabelingFunction;
  /**
   * sets mode for either directed or undirected graph
   */
  private final boolean directed;

  /**
   * constructor
   * @param graphHeadToString representation of graph heads
   * @param vertexToString representation of vertices
   * @param edgeLabelingFunction representation of edges
   * @param directed sets mode for either directed or undirected graph
   */
  public CanonicalAdjacencyMatrixBuilder(
    GraphHeadToString<GraphHead> graphHeadToString,
    VertexToString<Vertex> vertexToString,
    EdgeToString<Edge> edgeLabelingFunction,
    boolean directed
  ) {
    this.graphHeadToString = graphHeadToString;
    this.vertexToString = vertexToString;
    this.egeLabelingFunction = edgeLabelingFunction;
    this.directed = directed;
  }

  @Override
  public DataSet<String> execute(GraphCollection collection) {

    // 1-10.
    DataSet<GraphHeadString> graphHeadLabels = getGraphHeadStrings(collection);

    // 11. add empty head to prevent empty result for empty collection

    graphHeadLabels = graphHeadLabels
      .union(collection
        .getConfig()
        .getExecutionEnvironment()
        .fromElements(new GraphHeadString(GradoopId.get(), "")));

    // 12. label collection

    return graphHeadLabels
      .reduceGroup(new ConcatGraphHeadStrings());
  }

  /**
   * Created a dataset of (graph id, canonical label) pairs.
   *
   * @param collection input collection
   * @return (graph id, canonical label) pairs
   */
  public DataSet<GraphHeadString> getGraphHeadStrings(GraphCollection collection) {
    // 1. label graph heads
    DataSet<GraphHeadString> graphHeadLabels = collection.getGraphHeads()
      .map(graphHeadToString);

    // 2. label vertices
    DataSet<VertexString> vertexLabels = collection.getVertices()
      .flatMap(vertexToString);

    // 3. label edges
    DataSet<EdgeString> edgeLabels = collection.getEdges()
      .flatMap(egeLabelingFunction);

    if (directed) {
      // 4. combine labels of parallel edges
      edgeLabels = edgeLabels
        .groupBy(0, 1, 2)
        .reduceGroup(new MultiEdgeStringCombiner());

      // 5. extend edge labels by vertex labels

      edgeLabels = edgeLabels
        .join(vertexLabels)
        .where(0, 1).equalTo(0, 1) // graphId,sourceId = graphId,vertexId
        .with(new SourceStringUpdater())
        .join(vertexLabels)
        .where(0, 2).equalTo(0, 1) // graphId,targetId = graphId,vertexId
        .with(new TargetStringUpdater());

      // 6. extend vertex labels by outgoing vertex+edge labels

      DataSet<VertexString> outgoingAdjacencyListLabels =
        edgeLabels.groupBy(0, 1) // graphId, sourceId
          .reduceGroup(new OutgoingAdjacencyList());

      // 7. extend vertex labels by outgoing vertex+edge labels

      DataSet<VertexString> incomingAdjacencyListLabels =
        edgeLabels.groupBy(0, 2) // graphId, targetId
          .reduceGroup(new IncomingAdjacencyList());

      // 8. combine vertex labels

      vertexLabels = vertexLabels
        .leftOuterJoin(outgoingAdjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner<VertexString>())
        .leftOuterJoin(incomingAdjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner<VertexString>());
    } else {
    // undirected graph

      // 4. union edges with flipped edges and combine labels of parallel edges

      edgeLabels = edgeLabels
        .union(edgeLabels
          .map(new SwitchSourceTargetIds()))
        .groupBy(0, 1, 2)
        .reduceGroup(new MultiEdgeStringCombiner());

      // 5. extend edge labels by vertex labels

      edgeLabels = edgeLabels
        .join(vertexLabels)
        .where(0, 2).equalTo(0, 1) // graphId,targetId = graphId,vertexId
        .with(new TargetStringUpdater());

      // 6/7. extend vertex labels by vertex+edge labels

      DataSet<VertexString> adjacencyListLabels =
        edgeLabels.groupBy(0, 1) // graphId, sourceId
          .reduceGroup(new UndirectedAdjacencyList());

      // 8. combine vertex labels

      vertexLabels = vertexLabels
        .leftOuterJoin(adjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner<VertexString>());
    }

    // 9. create adjacency matrix labels

    DataSet<GraphHeadString> adjacencyMatrixLabels = vertexLabels
      .groupBy(0)
      .reduceGroup(new AdjacencyMatrix());

    // 10. combine graph labels

    graphHeadLabels = graphHeadLabels
      .leftOuterJoin(adjacencyMatrixLabels)
      .where(0).equalTo(0)
      .with(new LabelCombiner<GraphHeadString>());
    return graphHeadLabels;
  }
}
