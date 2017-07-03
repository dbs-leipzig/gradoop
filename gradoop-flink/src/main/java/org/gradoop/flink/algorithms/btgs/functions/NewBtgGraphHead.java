
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates an EPGM graph head representing a business transaction graphs.
 * @param <G> graph head type
 */
public class NewBtgGraphHead<G extends EPGMGraphHead>
  implements MapFunction<GradoopId, G>, ResultTypeQueryable<G> {

  /**
   * graph head factory
   */
  private final EPGMGraphHeadFactory<G> graphHeadFactory;

  /**
   * Constructor
   * @param graphHeadFactory graph head factory
   */
  public NewBtgGraphHead(EPGMGraphHeadFactory<G> graphHeadFactory) {
    this.graphHeadFactory = graphHeadFactory;
  }

  @Override
  public G map(GradoopId graphId) throws Exception {
    G graphHead = graphHeadFactory
      .createGraphHead(BusinessTransactionGraphs.BTG_LABEL);

    graphHead.setId(graphId);

    return graphHead;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public TypeInformation<G> getProducedType() {
    return TypeExtractor.createTypeInfo(graphHeadFactory.getType());
  }
}
