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
 * Created by stephan on 18.05.16.
 */

public class GraphGenToCollectionMapper<G extends EPGMGraphHead, V extends
  EPGMVertex, E extends EPGMEdge> implements
  FlatMapFunction<Tuple2<LongWritable,
    Text>, Tuple2<Object, Class>> {

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

  public GraphGenToCollectionMapper(EPGMGraphHeadFactory<G> graphHeadFactory,
    EPGMVertexFactory<V> vertecFactory, EPGMEdgeFactory<E> edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertecFactory;
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void flatMap(Tuple2<LongWritable, Text> longWritableTextTuple2,
    Collector<Tuple2<Object,Class>> collector) throws Exception {
    String graphString = longWritableTextTuple2.getField(1).toString();
    GraphGenReader graphGenreader = new GraphGenReader();
    graphGenreader.setContent(graphString);

    Collection<Tuple3<Long, Collection<Tuple2<Integer, String>>,
      Collection<Tuple3<Integer, Integer, String>>>> collection;
    collection=graphGenreader.getGraphCollection();

    Long lId = null;
    GradoopId id = null;
    String label = null;
    GradoopId targetId = null;
    GradoopIdSet graphs = new GradoopIdSet();
    Map<Integer, GradoopId> integerGradoopIdMapVertices;
    Tuple2<Integer, String> vertex;

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
        id = integerGradoopIdMapVertices.get((Integer)tupleEdge.getField(0));
        targetId = integerGradoopIdMapVertices.get((Integer)tupleEdge
          .getField(1));
        label = tupleEdge.getField(2).toString();
        collector.collect(new Tuple2<Object, Class>(this.edgeFactory
          .createEdge(label, id, targetId, graphs), EPGMEdge.class));
      }
    }
  }
}