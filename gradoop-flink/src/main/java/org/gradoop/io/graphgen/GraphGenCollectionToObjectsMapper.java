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

package org.gradoop.io.graphgen;

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.Collection;
import java.util.Map;

/**
 * Reads graph data from a 3-tuple collection. The collection consists of the
 * graph head, the vertices and the edges. The result of the mapping is a
 * tuple-2 dataset containing EPGMElements(EPGMGraphHead, EPGMVertex and
 * EPGMEdge) as Objects and the corresponding class for verification.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphGenCollectionToObjectsMapper<G extends EPGMGraphHead, V
  extends EPGMVertex, E extends EPGMEdge> implements
  FlatMapFunction<Tuple2<LongWritable, Text>, Tuple2<Object, Class>> {

  /**
   * Creates graph data objects
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Creates graph data objects
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Creates graph data objects
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates a flatmap function.
   *
   * @param graphHeadFactory graph head data factory
   * @param vertexFactory vertex data factory
   * @param edgeFactory edge data factory
   */
  public GraphGenCollectionToObjectsMapper(EPGMGraphHeadFactory<G>
    graphHeadFactory, EPGMVertexFactory<V> vertexFactory, EPGMEdgeFactory<E>
    edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  /**
   * Constructs a tuple-2 dataset containing EPGMElements(EPGMGraphHead,
   * EPGMVertex and EPGMEdge) as Objects and the corresponding class for
   * verification.
   *
   * @param inputTuple consists of a key(LongWritable) and a value(Text)
   * @param collector of tuple-2 of Object and Class
   * @throws Exception
   */
  @Override
  public void flatMap(Tuple2<LongWritable, Text> inputTuple,
    Collector<Tuple2<Object, Class>> collector) throws Exception {
    String graphString = inputTuple.getField(1).toString();
    GraphGenStringToCollection
      graphGenStringToCollection = new GraphGenStringToCollection();
    graphGenStringToCollection.setContent(graphString);

    Collection<Tuple3<Long, Collection<Tuple2<Integer, String>>,
      Collection<Tuple3<Integer, Integer, String>>>> collection;
    collection = graphGenStringToCollection.getGraphCollection();

    GradoopId id;
    String label;
    GradoopId targetId;
    GradoopIdSet graphs = new GradoopIdSet();
    Map<Integer, GradoopId> integerGradoopIdMapVertices;

    for (Tuple3<Long, Collection<Tuple2<Integer, String>>,
      Collection<Tuple3<Integer, Integer, String>>> tuple : collection) {
      integerGradoopIdMapVertices = new HashedMap();
      graphs.clear();

      id = GradoopId.get();
      graphs.add(id);
      collector.collect(new Tuple2<Object, Class>(this.graphHeadFactory
        .initGraphHead(id), EPGMGraphHead.class));

      for (Tuple2<Integer, String> tupleVertex : (Collection<Tuple2<Integer,
        String>>) tuple.getField(1)) {
        id = GradoopId.get();
        integerGradoopIdMapVertices.put((Integer) tupleVertex.getField(0), id);
        label = tupleVertex.getField(1).toString();
        collector.collect(new Tuple2<Object, Class>(this.vertexFactory
          .initVertex(id, label, graphs), EPGMVertex.class));
      }

      for (Tuple3<Integer, Integer, String> tupleEdge :
        (Collection<Tuple3<Integer, Integer, String>>) tuple.getField(2)) {
        id = integerGradoopIdMapVertices.get(tupleEdge.getField(0));
        targetId = integerGradoopIdMapVertices.get(tupleEdge
          .getField(1));
        label = tupleEdge.getField(2).toString();
        collector.collect(new Tuple2<Object, Class>(this.edgeFactory
          .createEdge(label, id, targetId, graphs), EPGMEdge.class));
      }
    }
  }
}
