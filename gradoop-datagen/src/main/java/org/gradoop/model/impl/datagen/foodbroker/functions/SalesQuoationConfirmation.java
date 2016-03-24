package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotationLine;

import java.util.Random;

public class SalesQuoationConfirmation
  implements JoinFunction<SalesQuotationLine, SalesQuotation, SalesQuotation> {

  private final Float probability;
  private final Float probabilityInfluence;

  public SalesQuoationConfirmation(Float probability, Float probabilityInfluence) {
    this.probability = probability;
    this.probabilityInfluence = probabilityInfluence;
  }

  @Override
  public SalesQuotation join(SalesQuotationLine salesQuotationLine,
    SalesQuotation salesQuotation) throws Exception {

    Float sentByInfluence =
      salesQuotation.getSentByQuality() * probabilityInfluence;

    Float sentToInfluence =
      salesQuotation.getSentByQuality() * probabilityInfluence;

    Float productInfluence =
      salesQuotationLine.getQuality() * probabilityInfluence;

    Float score = 1 - (new Random().nextFloat() +
      sentByInfluence + sentToInfluence + productInfluence);

    System.out.println(score);

    String status = SalesQuotation.STATUS_WON;

    if(score < probability) {
      status = SalesQuotation.STATUS_LOST;
    }

    salesQuotation.getProperties().set("status", status);

    return salesQuotation;
  }
}
