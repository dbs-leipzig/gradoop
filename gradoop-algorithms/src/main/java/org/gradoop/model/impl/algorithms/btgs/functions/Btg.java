package org.gradoop.model.impl.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.id.GradoopId;

public class Btg<G extends EPGMGraphHead>
  implements MapFunction<GradoopId, G>, ResultTypeQueryable<G> {

  private final EPGMGraphHeadFactory<G> graphHeadFactory;

  public Btg(EPGMGraphHeadFactory<G> graphHeadFactory) {
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
    return (TypeInformation<G>) TypeExtractor
      .createTypeInfo(graphHeadFactory.getType());
  }
}
