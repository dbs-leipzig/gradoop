
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Initializes a new graph head from a given GradoopId and its lineage information, e.g. the
 * source graph this one was created from.
 */
public class InitGraphHeadWithLineage
  implements MapFunction<Tuple2<GradoopId, GradoopId>, GraphHead>, ResultTypeQueryable<GraphHead> {
  /**
   * GraphHeadFactory
   */
  private final EPGMGraphHeadFactory<GraphHead> graphHeadFactory;

  /**
   * Constructor
   *
   * @param epgmGraphHeadFactory graph head factory
   */
  public InitGraphHeadWithLineage(EPGMGraphHeadFactory<GraphHead> epgmGraphHeadFactory) {
    this.graphHeadFactory = epgmGraphHeadFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GraphHead map(Tuple2<GradoopId, GradoopId> idTuple) {
    GraphHead head = graphHeadFactory.initGraphHead(idTuple.f0);
    Properties properties = Properties.createWithCapacity(1);
    properties.set("lineage", idTuple.f1);
    head.setProperties(properties);
    return head;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<GraphHead> getProducedType() {
    return TypeExtractor.createTypeInfo(graphHeadFactory.getType());
  }
}
