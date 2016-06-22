package org.gradoop.io.impl.tsv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * MapFunction to create EPGMEdges
 *
 * @param <E> EPGM edge type class
 */
public class TupleToEdge<E extends EPGMEdge> implements
  MapFunction<Tuple6<String, GradoopId, String, String, GradoopId, String>, E> {

  /**
   * Edge data factory.
   */
  private final EPGMEdgeFactory<E> edgeFactory;

  /**
   * Creates map function
   *
   * @param epgmEdgeFactory edge data factory
   */
  public TupleToEdge(EPGMEdgeFactory<E> epgmEdgeFactory){
    this.edgeFactory=epgmEdgeFactory;
  }

  /**
   * Creates edges based on tuple6 input
   *
   * @param lineTuple     read data from tsv input
   * @return              EPGMEdge
   * @throws Exception
   */
  @Override
  public E map(
    Tuple6<String, GradoopId, String, String, GradoopId, String> lineTuple)
    throws Exception {

    GradoopId edgeID = GradoopId.get();
    String edgeLabel = "";
    GradoopId sourceID = lineTuple.f1;
    GradoopId targetID = lineTuple.f4;
    PropertyList properties = PropertyList.create();
    GradoopIdSet graphs = GradoopIdSet.fromExisting(GradoopId.get());

    return edgeFactory.initEdge(edgeID, edgeLabel, sourceID, targetID,
      properties, graphs);
  }
}
