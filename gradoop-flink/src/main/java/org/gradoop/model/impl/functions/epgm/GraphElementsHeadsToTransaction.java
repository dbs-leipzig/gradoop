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

package org.gradoop.model.impl.functions.epgm;

    import org.apache.flink.api.common.functions.CoGroupFunction;
    import org.apache.flink.api.java.tuple.Tuple2;
    import org.apache.flink.util.Collector;
    import org.gradoop.model.api.EPGMEdge;
    import org.gradoop.model.api.EPGMGraphElement;
    import org.gradoop.model.api.EPGMGraphHead;
    import org.gradoop.model.api.EPGMVertex;
    import org.gradoop.model.impl.id.GradoopId;
    import org.gradoop.model.impl.tuples.GraphTransaction;

    import java.util.HashSet;
    import java.util.Iterator;
    import java.util.Set;

    /**
     * Generates a graphTransaction for a set of vertices, edges
     * and a graph head based on the same graph
     * @param <G> GraphHead
     * @param <V> EPGMVertex
     * @param <E> EPGMEdge
     */
public class GraphElementsHeadsToTransaction
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
    implements CoGroupFunction<Tuple2<GradoopId, EPGMGraphElement>, G,
    GraphTransaction<G, V, E>> {

  @Override
  public void coGroup(Iterable<Tuple2<GradoopId, EPGMGraphElement>>
    first, Iterable<G> second,
    Collector<GraphTransaction<G, V, E>> out) throws Exception {
    Set<V> vertices = new HashSet<V>();
    Set<E> edges = new HashSet<E>();
    G head = second.iterator().next();
    Iterator<Tuple2<GradoopId, EPGMGraphElement>> iterator = first.iterator();
    while (iterator.hasNext()) {
      EPGMGraphElement el = iterator.next().f1;
      if (el instanceof EPGMVertex) {
        vertices.add((V) el);
      } else if (el instanceof EPGMEdge) {
        edges.add((E) el);
      }
    }
    out.collect(new GraphTransaction<G, V, E>(head, vertices, edges));
    out.close();
  }
}
