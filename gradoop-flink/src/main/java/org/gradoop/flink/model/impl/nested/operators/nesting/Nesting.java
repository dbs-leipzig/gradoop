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

package org.gradoop.flink.model.impl.nested.operators.nesting;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.operators.BinaryOp;
import org.gradoop.flink.model.impl.nested.operators.nesting.functions.*;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.Hexaplet;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.functions.Hex0;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.functions.Hex4;
import org.gradoop.flink.model.impl.nested.operators.nesting.tuples.functions.HexMatch;
import org.gradoop.flink.model.impl.nested.operators.nesting.functions.MapGradoopIdAsVertex;
import org.gradoop.flink.model.impl.nested.utils.RepresentationUtils;

/**
 * Fuses each Logical Graph's set of vertices appearing in the same hypervertex into a single
 * vertex.
 *
 */
public class Nesting extends BinaryOp {

  /**
   * Debug mode
   */
  private final boolean debug = false;

  /**
   * GraphId to be associated to the graph that is going to be returned by this operator
   */
  private final GradoopId newGraphId;

  /**
   * Constructor for specifying the to-be-returned graph's head
   * @param newGraphId
   */
  public Nesting(GradoopId newGraphId) {
    this.newGraphId = newGraphId;
  }

  /**
   * The returned graph will have a new GradoopId
   */
  public Nesting() {
    this.newGraphId = GradoopId.get();
  }

  /**
   * Fusing the already-combined sources
   *
   * @param dataLake      Where to retrieve the original data (with no ids). This instance will be
   *                      also updated by the operation.
   * @param gU            Logical Graph defining the data lake
   * @param hypervertices Collection of elements representing which vertices will be merged into
   *                      a vertex
   * @return              A single merged graph
   */
  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake dataLake, IdGraphDatabase gU,
    IdGraphDatabase hypervertices) {
    // TODO:::: Missing in the theoric definition: creating a new header
    DataSet<GradoopId> gh = dataLake.asNormalizedGraph().getConfig().getExecutionEnvironment()
      .fromElements(newGraphId);
    // Associate each gid in hypervertices.H to the merged vertices
    //DataSet<GradoopId> subgraphIds = hypervertices.getGraphHeads();

    // Mark each vertex if either it's present or not in the final match
    // TODO       JOIN COUNT: (1)
    DataSet<Hexaplet> quads =
      gU.getGraphHeadToVertex()
        .leftOuterJoin(hypervertices.getGraphHeadToVertex())   // XXX: newVertexIdContainingOldVertex
        .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
        // If the vertex does not appear in the graph collection, the f2 element is null.
        // These vertices are the ones to be returned as vertices alongside with the new
        // graph heads
        .with(new AssociateAndMark());

    // Vertices to be returend within the IdGraphDatabase
    DataSet<Tuple2<GradoopId,GradoopId>> vertices = quads
      .groupBy(new Hex4())
      .reduceGroup(new CollectVertices(newGraphId));

    // The vertices appearing in a nested graph are the ones that induce the to-be-updated edges.
    DataSet<Hexaplet> verticesPromotingEdgeUpdate = quads.filter(new GetVerticesToBeNested());

    // Edges to return and update are the ones that do not appear in the collection
    // TODO       JOIN COUNT: (2) -> NotInGraphBroadcast (a)
    DataSet<Hexaplet> edgesToUpdateOrReturn = RepresentationUtils
      // Each edge is associated to each possible graph
      .datalakeEdgesToQuadMatchingSource(dataLake)
      // (1) Now, we want to select the edge information only for the graphs in gU
      .joinWithTiny(gU.getGraphHeadToEdge())
      .where(new Hex0()).equalTo(new Value1Of2<>())
      .with(new CombineGraphBelongingInformation())
      .distinct(0)
      // (2) Mimicking the NotInGraphBroadcast
      .leftOuterJoin(hypervertices.getGraphHeadToEdge())
      .where(new Hex4()).equalTo(new Value1Of2<>())
      .with(new QuadEdgeDifference());

    // I have to only add the edges that are matched and updated
    // TODO       JOIN COUNT: (2)
    DataSet<Hexaplet> updatedEdges = edgesToUpdateOrReturn
      // Update the vertices' source
      .leftOuterJoin(verticesPromotingEdgeUpdate)
      .where(new HexMatch()).equalTo(new HexMatch())
      .with(new UpdateEdgeSource(true))
      // Now start the match with the targets
      .map(new DoQuadMatchTarget())
      .leftOuterJoin(verticesPromotingEdgeUpdate)
      .where(new HexMatch()).equalTo(new HexMatch())
      .with(new UpdateEdgeSource(false));

    DataSet<Tuple2<GradoopId, GradoopId>> v0 = gh.cross(gh);
    vertices = vertices.union(v0);

    DataSet<Tuple2<GradoopId,GradoopId>> preliminaryEdge = updatedEdges
      .map(new CollectEdgesPreliminary());

    DataSet<Tuple2<GradoopId,GradoopId>> edgesToProvide = preliminaryEdge
      .flatMap(new CollectEdges(newGraphId,false));

    // Create new edges in the dataLake
    DataSet<Edge> newlyCreatedEdges = dataLake.getEdges()
      // Associate each edge to each new edge where he has generated from
      .coGroup(updatedEdges)
      .where(new Id<>()).equalTo(new Hex0())
      .with(new DuplicateEdgeInformations());
    dataLake.incrementalUpdateEdges(newlyCreatedEdges,edgesToProvide);
    dataLake.incrementalUpdateVertices(gh.map(new MapGradoopIdAsVertex()),gh.cross(gh));

    // Edges to be set within the IdGraphDatabase
    DataSet<Tuple2<GradoopId,GradoopId>> edges = preliminaryEdge
      .flatMap(new CollectEdges(newGraphId,true));

    return new IdGraphDatabase(gh,vertices,edges);
  }

  @Override
  public String getName() {
    return Nesting.class.getName();
  }

}
