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

/**
 * f0 = Edge => (Old) Edge Id     |  Vertex => Data Lake id
 * f1 = Edge =>       Edge Source |  Vertex => Vertex Id
 * f2 = Matching space.
 *      Edge => E. Source or Dst  |  Vertex => Graph Collection id
 *                                             At a later step, to be changed with its id
 * f3 = Edge Target (only if this represents an edge)
 * f4 = Edge: If the edge has been updated, contains the new edge's id. Otherwise, it is NULL_ID
 *      Vertex: the gc id
 * 45 = Edge: Data Lake id        |  Vertex => Null
 */
public class Hexaplet extends Tuple6<GradoopId, GradoopId, GradoopId, GradoopId, GradoopId, GradoopId> {

  /**
   * Defines the quad from the joining of the vertices
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
   * Defines a Exaplet from the edge.
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
   * Sets the match field (f2) with the target element
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
   * If the Exaplet represents a vertex, then this method is called when the fe f4 field
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
   * Updates the edge's source
   * @param source  New source id (summarized vertex)
   */
  public void setSource(GradoopId source) {
    if (isEdge()) {
      f1 = source;
    }
  }

  /**
   * Updates the edge's target
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
