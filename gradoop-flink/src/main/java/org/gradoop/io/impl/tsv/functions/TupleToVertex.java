package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.HashMap;

/**
 * FlatMapFunction to create EPGMVertices
 *
 * @param <V> EPGM vertex type class
 */
public class TupleToVertex <V extends EPGMVertex> implements
  FlatMapFunction<Tuple6<String, GradoopId, String, String, GradoopId, String>, V> {

  /**
   * Creates vertex data objects.
   */
  private final EPGMVertexFactory<V> vertexFactory;

  /**
   * Creates flatmap function
   *
   * @param vertexFactory vertex data factory
   */
  public TupleToVertex (EPGMVertexFactory<V> vertexFactory){
    this.vertexFactory = vertexFactory;
  }

  /**
   * Creates EPGMVertices based on Tuple6 input
   *
   * @param lineTuple   read data from tsv input
   * @param collector   flatMap collector
   * @throws Exception
   */
  @Override
  public void flatMap(
    Tuple6<String, GradoopId, String, String, GradoopId, String> lineTuple,
    Collector<V> collector) throws Exception {

    collector.collect(createVertex(lineTuple.f1, lineTuple.f0, lineTuple.f2));
    collector.collect(createVertex(lineTuple.f4, lineTuple.f3, lineTuple.f5));

  }

  /**
   * Method to initialize EPGMVertices
   *
   * @param vertexId    GradoopId
   * @param originId    original id read from tsv input
   * @param language    language property read from tsv input
   * @return            initialized EPGMVertex
   */
  private V createVertex(GradoopId vertexId, String originId, String language){

    java.lang.String label = "";
    HashMap<java.lang.String, Object> properties = new HashMap<>();
    properties.put("lan", language);
    properties.put("id", originId);
    PropertyList propertyList = PropertyList.createFromMap(properties);
    GradoopIdSet graphs = GradoopIdSet.fromExisting(GradoopId.get());

    return vertexFactory.initVertex(vertexId, label, propertyList,
      graphs);
  }
}
