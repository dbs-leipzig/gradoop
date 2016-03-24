package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;


public class SalesQuotation
  implements MapFunction<Long, TransactionalDataObject> {

  public static final String STATUS_KEY = "status";
  public static final String STATUS_OPEN = "open";
  public static final String STATUS_LOST = "lost";
  public static final String STATUS_WON = "won";
  public static final String SENT_BY = "sentBy";
  public static final String SENT_TO = "sentTo";
  private static final String CLASS_NAME = "SalesQuotation";

  @Override
  public TransactionalDataObject map(Long caseId) throws Exception {
    TransactionalDataObject salesQuotation = new TransactionalDataObject();

    salesQuotation.setCaseId(caseId);
    salesQuotation.getProperties().set("bid", "QTN" + caseId.toString());
    salesQuotation.getProperties().set(STATUS_KEY, STATUS_OPEN);
    salesQuotation.setLabel(SalesQuotation.CLASS_NAME);

    return salesQuotation;
  }
}
