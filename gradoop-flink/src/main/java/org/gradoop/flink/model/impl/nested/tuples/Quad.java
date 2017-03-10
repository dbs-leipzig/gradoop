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

package org.gradoop.flink.model.impl.nested.tuples;

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
public class Quad extends Tuple6<GradoopId, GradoopId, GradoopId, GradoopId, GradoopId, GradoopId> {

  /**
   * Defines the quad from the joining of the vertices
   * @param fromDataLake          Vertex appearing in the data lake (if present)
   * @param fromGraphCollection   Vertex appearing in the graph collection (if present)
   */
  public void update(Tuple2<GradoopId, GradoopId> fromDataLake,
                     Tuple2<GradoopId, GradoopId> fromGraphCollection)  {
    f3 = GradoopId.NULL_VALUE;
    f4 = GradoopId.NULL_VALUE;
    f5 = GradoopId.NULL_VALUE;
    if (fromDataLake!=null) {
      f0 = fromDataLake.f0;
      f1 = fromDataLake.f1;
    } else {
      f0 = GradoopId.NULL_VALUE;
    }
    if (fromGraphCollection!=null) {
      if (fromDataLake.f1.equals(fromDataLake.f1))
        f2 = fromGraphCollection.f0;
      else {
        f2 = GradoopId.NULL_VALUE;
        f1 = GradoopId.NULL_VALUE;
        f0 = GradoopId.NULL_VALUE;
      }
    } else {
      f2 = GradoopId.NULL_VALUE;
    }
  }



  /**
   * Defines a quad from the edge.
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
   * Defines a quad from the edge.
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
   * @param vertexInformation
   */
  public void update(Tuple2<GradoopId, GradoopId> vertexInformation) {
    f0 = vertexInformation.f0;
    f1 = vertexInformation.f1;
    f2 = GradoopId.NULL_VALUE;
    f3 = GradoopId.NULL_VALUE;
    f4 = GradoopId.NULL_VALUE;
    f5 = GradoopId.NULL_VALUE;
  }

  public boolean isEdge() {
    return !f3.equals(GradoopId.NULL_VALUE);
  }

  public void updateToMatchWithSource() {
    if (isEdge()) {
      setMatchWith(f1);
    }
  }

  public void updateToMatchWithTarget() {
    if (isEdge()) {
      setMatchWith(f3);
    }
  }

  /**
   * Match an edge with a vertex id only if the vertex appears in a graph collection
   */
  public void setGCAsVertexIdAndStoreGCInf4() {
    if ((!isEdge())&&(appearsInGraphCollection())) {
      f4 = getMatch();            // The new information with which the edge has to be
      setMatchWith(getVertexId());
    }
  }

  public GradoopId getWhereTheVertexComesFromWithinTheDataLake() {
    return f0;
  }

  public GradoopId getVertexId() {
    return f1;
  }

  public GradoopId getWhereTheVertexComesFromWithinTheGraphCollection() {
    return f2;
  }

  public boolean correctMatch() {
    return !f1.equals(GradoopId.NULL_VALUE);
  }

  public boolean appearsInGraphCollection() {
    return !f2.equals(GradoopId.NULL_VALUE);
  }

  public boolean appearsInDataLake() {
    return !f0.equals(GradoopId.NULL_VALUE);
  }

  private void setMatchWith(GradoopId val) {
    f2 = val;
  }

  public GradoopId getMatch() {
    return f2;
  }

  public void setSource(GradoopId source) {
    if (isEdge()) {
      f1 = source;
    }
  }

  public void setTarget(GradoopId source) {
    if (isEdge()) {
      f3 = source;
    }
  }

  public void setNewId(GradoopId newId) {
    if (isEdge()) {
      f4 = newId;
    }
  }

  public GradoopId getSource() {
    return f1;
  }

  public GradoopId getTarget() {
    return f3;
  }
}
