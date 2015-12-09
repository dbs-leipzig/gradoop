package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Adds the new graph id's to the graphs
 *
 * @param <G> EPGM graph type
 */
public  class AddNewGraphsToGraphMapper<G extends EPGMGraphHead> implements
  MapFunction<Tuple1<GradoopId>, G>, ResultTypeQueryable<G> {
  /**
   * EPGMGraphHeadFactory
   */
  private EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Constructor
   *
   * @param graphHeadFactory actual EPGMGraphHeadFactory
   */
  public AddNewGraphsToGraphMapper(EPGMGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public G map(Tuple1<GradoopId> idTuple) {
    GradoopId id = idTuple.f0;
    return graphHeadFactory.initGraphHead(id, "split graph " + id);
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<G> getProducedType() {
    return (TypeInformation<G>) TypeExtractor
      .createTypeInfo(graphHeadFactory.getType());
  }
}
