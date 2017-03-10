/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.nested.algorithms.nesting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.nested.algorithms.BinaryOp;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.functions.DuplicateEdgeInformations;
import org.gradoop.flink.model.impl.nested.functions.GetVerticesToBeNested;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.functions.AssociateAndMark;
import org.gradoop.flink.model.impl.nested.functions.CollectEdges;
import org.gradoop.flink.model.impl.nested.functions.CollectVertices;
import org.gradoop.flink.model.impl.nested.functions.DoQuadMatchTarget;
import org.gradoop.flink.model.impl.nested.functions.UpdateEdgeSource;
import org.gradoop.flink.model.impl.nested.tuples.Quad;
import org.gradoop.flink.model.impl.nested.tuples.Quad0;
import org.gradoop.flink.model.impl.nested.tuples.Quad4;
import org.gradoop.flink.model.impl.nested.tuples.QuadEdgeDifference;
import org.gradoop.flink.model.impl.nested.tuples.QuadMatch;
import org.gradoop.flink.model.impl.nested.utils.RepresentationUtils;

/**
 * Fuses each Logical Graph's set of vertices appearing in the same hypervertex into a single
 * vertex.
 *
 */
public class GraphNestingWithModel extends BinaryOp {

  private final boolean debug = true;

  /**
   * Fusing the already-combined sources
   *
   * @param gU            Logical Graph defining the data lake
   * @param hypervertices Collection of elements representing which vertices will be merged into
   *                      a vertex
   * @return              A single merged graph
   */
  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake dataLake, IdGraphDatabase gU,
    IdGraphDatabase hypervertices) {

    GradoopId newGraphId = GradoopId.get();

    // TODO:::: Missing in the theoric definition: creating a new header
    DataSet<GradoopId> gh = gU.getGraphHeads();
    // Associate each gid in hypervertices.H to the merged vertices
    //DataSet<GradoopId> subgraphIds = hypervertices.getGraphHeads();

    // Mark each vertex if either it's present or not in the final match
    // TODO       JOIN COUNT: (1)
    DataSet<Quad> quads =
      gU.getGraphHeadToVertex()
        .leftOuterJoin(hypervertices.getGraphHeadToVertex())   // XXX: newVertexIdContainingOldVertex
        .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
        // If the vertex does not appear in the graph collection, the f2 element is null.
        // These vertices are the ones to be returned as vertices alongside with the new
        // graph heads
        .with(new AssociateAndMark());



    // TODO --->
    // Vertices to be returend within the IdGraphDatabase
    DataSet<Tuple2<GradoopId,GradoopId>> vertices = quads
      .groupBy(new Quad4())
      .reduceGroup(new CollectVertices(newGraphId));

    // The vertices appearing in a nested graph are the ones that induce the to-be-updated edges.
    DataSet<Quad> verticesPromotingEdgeUpdate = quads.filter(new GetVerticesToBeNested());

    // Edges to return and update are the ones that do not appear in the collection
    // TODO       JOIN COUNT: (1) -> NotInGraphBroadcast (a)
    DataSet<Quad> edgesToUpdateOrReturn = RepresentationUtils
      // Each edge is associated to each possible graph
      .datalakeEdgesToQuadMatchingSource(dataLake)
      // Now, we want to select the edge information only for the graphs in gU
      .joinWithTiny(gU.getGraphHeadToEdge())
      .where(new Quad0()).equalTo(new Value1Of2<>())
      .with(new CombineGraphBelongingInformation())
      .distinct(0)
      .leftOuterJoin(hypervertices.getGraphHeadToEdge())
      .where(new Quad4()).equalTo(new Value1Of2<>())
      .with(new QuadEdgeDifference());

    // I have to only add the edges that are matched and updated
    // TODO       JOIN COUNT: (2)
    DataSet<Quad> updatedEdges = edgesToUpdateOrReturn
      // Update the vertices' source
      .leftOuterJoin(verticesPromotingEdgeUpdate)
      .where(new QuadMatch()).equalTo(new QuadMatch())
      .with(new UpdateEdgeSource(true))
      // Now start the match with the targets
      .map(new DoQuadMatchTarget())
      .leftOuterJoin(verticesPromotingEdgeUpdate)
      .where(new QuadMatch()).equalTo(new QuadMatch())
      .with(new UpdateEdgeSource(false));

    // Edges to be set within the IdGraphDatabase
    DataSet<Tuple2<GradoopId,GradoopId>> edges = updatedEdges
      .groupBy(new Quad4())
      .reduceGroup(new CollectEdges(newGraphId));

    try {
      if (debug) {
        System.out.println("HeadToVertex Input: " + hypervertices.getGraphHeadToVertex().collect());
        System.out.println("Quads: " + quads.collect());
        System.out.println("ToRetVertexAssoc: " + vertices.collect());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Create new edges in the dataLake
    DataSet<Edge> newlyCreatedEdges = dataLake.getEdges()
      // Associate each edge to each new edge where he has generated from
      .coGroup(updatedEdges)
      .where(new Id<>()).equalTo(new Quad0())
      .with(new DuplicateEdgeInformations());
    dataLake.updateEdges(newlyCreatedEdges,edges);

    return new IdGraphDatabase(gh,vertices,edges);
  }

  @Override
  public String getName() {
    return GraphNestingWithModel.class.getName();
  }

}
