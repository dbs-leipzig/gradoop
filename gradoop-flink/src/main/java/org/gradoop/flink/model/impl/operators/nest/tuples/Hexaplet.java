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

package org.gradoop.flink.model.impl.operators.nest.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.nest.functions.AsEdgesMatchingSource;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateAndMark;
import org.gradoop.flink.model.impl.operators.nest.functions.CombineGraphBelongingInformation;
import org.gradoop.flink.model.impl.operators.nest.functions.DoHexMatchTarget;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdgeSource;

/**
 * This class describes the state of the matching at different computation phase, either for
 * vertices or edges. Either for vertices and edges, the field f2 corresponds to the value
 * we want to match within the join operations.
 *
 * 1) VERTICES
 *     a. Initializing a match [1st round] between vertices (graph vs. graph collection).
 *        The match is OK (that is, the same vertex id are matched)
 *        {@link AssociateAndMark}
 *
 *        f0 = graph head id from the search graph id
 *        f1 = vertex id from the search graph id and the graph collection
 *        f2 = graph head id from the graph collection. The value is null if the vertex
 *             does not appear within the collection
 *
 *     b. Initializing a match [2nd round] between vertices (graph vs. graph collection).
 *        The match is OK (that is, the same vertex id are matched)
 *
 *        f0 = graph head id from the search graph id
 *        f1 = vertex id from the search graph id and the graph collection
 *        f2 = vertex id from the search graph id and the graph collection. The value is null
 *             if it does not appear within the collection.
 *        f4 = the value of the match is f2 from [1a], that is "graph head id from the graph
 *        collection". This will become a vertex of the nested graph.
 *
 * 2) EDGES
 *     a. Initializing the edge match on the edges appearing in the flattened graph.
 *        {@link AsEdgesMatchingSource}. f2 is the field where the join match is performed,
 *        so within the initialization phase we want to check if the current edge has a
 *        source vertex that is going to be summarized by the nesting operation.
 *
 *        In this phase we want to remember the old edge id {@code f0} that carries out the
 *        information inherited by the new edge {@code f4}. The reason is that we do not want to
 *        duplicate data information, and that we want to associate the data to the edge only at
 *        a later step, thus reducing the size of the exchanged messages within the nodes.
 *
 *        f0 = edge id from the flattened graph. This information keeps stable throughout the whole
 *             computation.
 *        f1 = vertex source id
 *        f2 = becomes the vertex id from the search graph that we want to match with the
 *
 *        f3 = vertex target id
 *
 *     b. {@link CombineGraphBelongingInformation}
 *
 *        f5 = graph where the edge appears in the flattened graph
 *
 *     c. {@link UpdateEdgeSource} In this phase we create the new edges: this operation is
 *        carried out by matching either the source (true) or the target (false) vertex for
 *        each search graph vertex appearing in the graph collection.
 *
 *        f0 = updated each time the edge belongs to a new edge. Consequently, it defines
 *             the new id for the new edge
 *        f1 = eventually updated to the new vertex source (
 *        f3 = eventually updated to the new vertex target
 *        f4 = new edge id
 *
 *     d. {@link DoHexMatchTarget} The match is now set between the target id and the
 *        summarized vertex. Then Phase 2c is repeated again, but now with the target vertex.
 *
 *
 * f0 = Edge => (Old) Edge Id     |  Vertex => search graph id
 * f1 = Edge =>       Edge Source |  Vertex => Vertex Id
 * f2 = Matching space.
 *      Edge => E. Source or Dst  |  Vertex => Graph Collection id
 *                                             At a later step, to be changed with its id
 * f3 = Edge Target (only if this represents an edge)
 * f4 = Edge: If the edge has been updated, contains the new edge's id. Otherwise, it is NULL_ID
 *      Vertex: the graph collection id
 * f5 = Edge: flattened graph id  |  Vertex => Null
 */
public class Hexaplet extends Tuple6<GradoopId, GradoopId, GradoopId, GradoopId, GradoopId, GradoopId> {

  /**
   * Defines the quad from the joining of the vertices [Phase 1a]
   * @param fromGraph          Vertex appearing in the data lake (if present)
   * @param fromGraphCollection   Vertex appearing in the graph collection (if present)
   */
  public void update(Tuple2<GradoopId, GradoopId> fromGraph,
                     Tuple2<GradoopId, GradoopId> fromGraphCollection)  {
    f3 = GradoopId.NULL_VALUE;
    f4 = GradoopId.NULL_VALUE;
    f5 = GradoopId.NULL_VALUE;
    if (fromGraph != null) {
      f0 = fromGraph.f0;
      f1 = fromGraph.f1;
    } else {
      f0 = GradoopId.NULL_VALUE;
    }
    if (fromGraphCollection != null) {
      if (fromGraph != null && fromGraph.f1.equals(fromGraphCollection.f1)) {
        f2 = fromGraphCollection.f0;
      } else {
        f2 = GradoopId.NULL_VALUE;
        f1 = GradoopId.NULL_VALUE;
        f0 = GradoopId.NULL_VALUE;
      }
    } else {
      f2 = GradoopId.NULL_VALUE;
    }
  }



  /**
   * Defines a Exaplet from the edge.
   * @param pojo      Elements that has to be extracted
   */
  public void update(Edge pojo) {
    f0 = pojo.getId();
    f1 = pojo.getSourceId();
    f2 = GradoopId.MAX_VALUE;
    f3 = pojo.getTargetId();
    f4 = GradoopId.NULL_VALUE;
    f5 = GradoopId.NULL_VALUE;
  }

  /**
   * Defines a Exaplet from the edge. [Phase 2a]
   * @param pojo              Elements that has to be extracted
   * @param matchWithSource   If the matching element is the source or the target
   */
  public void update(Edge pojo, boolean matchWithSource) {
    f0 = pojo.getId();
    f1 = pojo.getSourceId();
    f3 = pojo.getTargetId();
    f2 = matchWithSource ? f1 : f3;
    f4 = GradoopId.NULL_VALUE;
    f5 = GradoopId.NULL_VALUE;
  }

  /**
   * Defines the vertex information
   * @param vertexInformation Vertex information to be associated
   */
  public void update(Tuple2<GradoopId, GradoopId> vertexInformation) {
    f0 = vertexInformation.f0;
    f1 = vertexInformation.f1;
    f2 = GradoopId.NULL_VALUE;
    f3 = GradoopId.NULL_VALUE;
    f4 = GradoopId.NULL_VALUE;
    f5 = GradoopId.NULL_VALUE;
  }

  /**
   * Returns…
   * @return if the Exaplet represents an edge or not
   */
  public boolean isEdge() {
    return !f3.equals(GradoopId.NULL_VALUE);
  }

  /**
   * Sets the match field (f2) with the source element
   */
  public void updateToMatchWithSource() {
    if (isEdge()) {
      setMatchWith(f1);
    }
  }

  /**
   * Sets the match field (f2) with the target element [Phase 2d]
   */
  public void updateToMatchWithTarget() {
    if (isEdge()) {
      setMatchWith(f3);
    }
  }

  /**
   * Match an edge with a vertex id only if the vertex appears in a graph collection
   */
  public void setGCAsVertexIdAndStoreGCInf4() {
    if ((!isEdge()) && (appearsInGraphCollection())) {
      f4 = getMatch();            // The new information with which the edge has to be
      setMatchWith(getVertexId());
    }
  }

  /**
   * If the Exaplet represents a vertex, then it returns…
   * @return from which graph within the datalake it comes from
   */
  public GradoopId getWhereTheVertexComesFromWithinTheDataLake() {
    return f0;
  }

  /**
   * If the Exaplet represents a vertex, then it returns…
   * @return  …its id
   */
  public GradoopId getVertexId() {
    return f1;
  }

  /**
   * If the current Exaplet represents a vertex, it returns…
   * @return  if the vertex appears in a Graph Collection that is involved in the nest operation
   */
  public boolean appearsInGraphCollection() {
    return !f2.equals(GradoopId.NULL_VALUE);
  }

  /**
   * If the Exaplet represents a vertex, then this method is called when the f4 field
   * representing the GraphCollection where it appears is set. So this vertex will be marked as
   * one to be replaced by an aggregated vertex.
   *
   * If the Exaplet represents an edge, then the match will be with a vertex appearing in a
   * GraphCollection, and could be either its source or target id.
   * @param val The aforementioned value to be set
   */
  private void setMatchWith(GradoopId val) {
    f2 = val;
  }

  /**
   * The field containing the match value between vertices (id) and edges
   * (src or destination). When this value is null, it means that it is a vertex
   * that has not undergone the summarization process.
   * @return  The value of the match
   */
  public GradoopId getMatch() {
    return f2;
  }

  /**
   * Updates the edge's source [Phase 2c]
   * @param source  New source id (summarized vertex)
   */
  public void setSource(GradoopId source) {
    if (isEdge()) {
      f1 = source;
    }
  }

  /**
   * Updates the edge's target [Phase 2c]
   * @param source  New target id (summarized vertex)
   */
  public void setTarget(GradoopId source) {
    if (isEdge()) {
      f3 = source;
    }
  }

  /**
   * When an edge is updated, then its id will change, too (because this
   * update means that a new edge has to be created from the old one)
   *
   * @param newId New edge id
   */
  public void setNewId(GradoopId newId) {
    if (isEdge()) {
      f4 = newId;
    }
  }

  /**
   * If the Exaplet represents an edge, then it returns…
   * @return  its source
   */
  public GradoopId getSource() {
    return f1;
  }

  /**
   * If the Exaplet represents an edge, then it returns…
   * @return  its target
   */
  public GradoopId getTarget() {
    return f3;
  }
}
