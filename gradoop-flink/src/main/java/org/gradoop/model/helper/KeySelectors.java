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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.helper;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.EdgeData;
import org.gradoop.model.GraphData;
import org.gradoop.model.VertexData;
import org.gradoop.model.impl.Subgraph;

/**
 * Key selectors used in the operator implementations.
 */
public class KeySelectors {
  /**
   * Returns the unique graph identifier.
   */
  public static class GraphKeySelector<GD extends GraphData> implements
    KeySelector<Subgraph<Long, GD>, Long> {
    @Override
    public Long getKey(Subgraph<Long, GD> g) throws Exception {
      return g.f0;
    }
  }

  /**
   * Used for distinction of vertices based on their unique id.
   */
  public static class VertexKeySelector<VD extends VertexData> implements
    KeySelector<Vertex<Long, VD>, Long> {
    @Override
    public Long getKey(Vertex<Long, VD> v) throws Exception {
      return v.f0;
    }
  }

  /**
   * Used for distinction of edges based on their unique id.
   */
  public static class EdgeKeySelector<ED extends EdgeData> implements
    KeySelector<Edge<Long, ED>, Long> {
    @Override
    public Long getKey(Edge<Long, ED> e) throws Exception {
      return e.f2.getId();
    }
  }

  /**
   * Used to select the source vertex id of an edge.
   */
  public static class EdgeSourceVertexKeySelector<ED extends EdgeData>
    implements
    KeySelector<Edge<Long, ED>, Long> {
    @Override
    public Long getKey(Edge<Long, ED> e) throws Exception {
      return e.getSource();
    }
  }

  /**
   * Used to select the target vertex id of an edge.
   */
  public static class EdgeTargetVertexKeySelector<ED extends EdgeData>
    implements
    KeySelector<Edge<Long, ED>, Long> {
    @Override
    public Long getKey(Edge<Long, ED> e) throws Exception {
      return e.getTarget();
    }
  }
}
