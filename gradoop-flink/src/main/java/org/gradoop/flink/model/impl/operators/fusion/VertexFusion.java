/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.BinaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.graphcontainment.NotInGraphsBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.fusion.functions.CoGroupAssociateOldVerticesWithNewIds;
import org.gradoop.flink.model.impl.operators.fusion.functions.CoGroupGraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.fusion.functions.FlatJoinSourceEdgeReference;
import org.gradoop.flink.model.impl.operators.fusion.functions.LeftElementId;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapFunctionAddGraphElementToGraph2;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapGraphHeadForNewGraph;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapVertexToPairWithGraphId;
import org.gradoop.flink.model.impl.operators.fusion.functions.MapVerticesAsTuplesWithNullId;

import static org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast.GRAPH_IDS;


/**
 * Fusion is a binary operator taking two graphs: a search graph (first parameter) and a
 * pattern graph (second parameter) [This means that this is not a symmetric operator
 * (F(a,b) != F(b,a))]. The general idea of this operator is that everything that appears
 * in the pattern graph is fused into a single vertex into the search graph. A new logical graph
 * is returned.
 *
 * 1) If any search graph is empty, an empty result will always be returned
 * 2) If a pattern graph is empty, then the search graph will always be returned
 * 3) The vertices of the pattern graph appearing in the first parameter are replaced by a
 *    new vertex. The old edges of the search graph are updated in the final result so that
 *    each vertex, either pointing to a to-be-fused vertex or starting from a to-be-fused vertex,
 *    are respectively updated as either pointing or starting from the fused vertex.
 * 4) Pattern graph's edges are also taken into account: any edge between to-be-fused vertices
 *    appearing in the search graph that are not expressed in the pattern graph are
 *    rendered as hooks over the fused vertex
 */
public class VertexFusion implements BinaryGraphToGraphOperator {

  /**
   * Fusing the already-combined sources
   *
   * @param searchGraph            Logical Graph defining the data lake
   * @param graphPatterns Collection of elements representing which vertices will be merged into
   *                      a vertex
   * @return              A single merged graph
   */
  @Override
  public LogicalGraph execute(LogicalGraph searchGraph, LogicalGraph graphPatterns) {

    // Missing in the theoric definition: creating a new header
    GradoopId newGraphid = GradoopId.get();

    DataSet<GraphHead> gh = searchGraph.getGraphHead()
      .map(new MapGraphHeadForNewGraph(newGraphid));

    DataSet<GradoopId> subgraphIds = graphPatterns.getGraphHead()
      .map(new Id<>());

    // PHASE 1: Induced Subgraphs
    // Associate each vertex to its graph id
    DataSet<Tuple2<Vertex, GradoopId>> vWithGid = graphPatterns.getVertices()
      .coGroup(searchGraph.getVertices())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new LeftSide<>())
      .flatMap(new MapVertexToPairWithGraphId());

    // Associate each gid in hypervertices.H to the merged vertices
    DataSet<Tuple2<Vertex, GradoopId>> nuWithGid  = graphPatterns.getGraphHead()
      .map(new CoGroupGraphHeadToVertex());

    // PHASE 2: Recreating the vertices
    DataSet<Vertex> vi = searchGraph.getVertices()
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(subgraphIds, GRAPH_IDS);

    // PHASE 3: Recreating the edges
    DataSet<Tuple2<Vertex, GradoopId>> idJoin = vWithGid
      .coGroup(nuWithGid)
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new CoGroupAssociateOldVerticesWithNewIds())
      .union(vi.map(new MapVerticesAsTuplesWithNullId()));

    DataSet<Vertex> vToRet = nuWithGid
      .coGroup(vWithGid)
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      .with(new LeftSide<>())
      .map(new Value0Of2<>())
      .union(vi)
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    DataSet<Edge> edges = searchGraph.getEdges()
      .filter(new NotInGraphsBroadcast<>())
      .withBroadcastSet(subgraphIds, GRAPH_IDS)
      .leftOuterJoin(idJoin)
      .where(new SourceId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(true))
      .leftOuterJoin(idJoin)
      .where(new TargetId<>()).equalTo(new LeftElementId<>())
      .with(new FlatJoinSourceEdgeReference(false))
      .map(new MapFunctionAddGraphElementToGraph2<>(newGraphid));

    return searchGraph.getConfig().getLogicalGraphFactory().fromDataSets(gh, vToRet, edges);
  }

  @Override
  public String getName() {
    return VertexFusion.class.getName();
  }
}
