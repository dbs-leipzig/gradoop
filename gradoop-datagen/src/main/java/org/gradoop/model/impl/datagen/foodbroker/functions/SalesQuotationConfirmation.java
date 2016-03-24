package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Random;

public class SalesQuotationConfirmation
  implements JoinFunction<Tuple2<GradoopId, Short >, TransactionalDataObject,
  TransactionalDataObject> {

  private final Float probability;
  private final Float probabilityInfluence;

  public SalesQuotationConfirmation(Float probability, Float probabilityInfluence) {
    this.probability = probability;
    this.probabilityInfluence = probabilityInfluence;
  }

  @Override
  public TransactionalDataObject join(
    Tuple2<GradoopId, Short> productQuality,
    TransactionalDataObject salesQuotation) throws Exception {

    Float sentByInfluence = salesQuotation.getQualities()
      .get(SalesQuotation.SENT_BY) * probabilityInfluence;

    Float sentToInfluence = salesQuotation.getQualities()
      .get(SalesQuotation.SENT_TO) * probabilityInfluence;

    Float productInfluence =
      productQuality.f1 * probabilityInfluence;

    Float score = 1 - (new Random().nextFloat() +
      sentByInfluence + sentToInfluence + productInfluence);

    String status = SalesQuotation.STATUS_WON;

    if(score < probability) {
      status = SalesQuotation.STATUS_LOST;
    }

    salesQuotation.getProperties().set("status", status);

    return salesQuotation;
  }
}
