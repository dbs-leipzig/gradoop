
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Initializes a new graph head from a given GradoopId.
 */
public class InitGraphHead implements MapFunction<Tuple1<GradoopId>, GraphHead>,
  ResultTypeQueryable<GraphHead> {
  /**
   * GraphHeadFactory
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Constructor
   *
   * @param epgmGraphHeadFactory graph head factory
   */
  public InitGraphHead(EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory) {
    this.graphHeadFactory = epgmGraphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead map(Tuple1<GradoopId> idTuple) {
    return graphHeadFactory.initGraphHead(idTuple.f0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<GraphHead> getProducedType() {
    return TypeExtractor.createTypeInfo(graphHeadFactory.getType());
  }
}
