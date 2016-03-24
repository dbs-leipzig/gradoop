package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;

import java.util.Random;


public class SalesQuotationLine implements
  FlatMapFunction<TransactionalDataObject, TransactionalDataObject> {

  public static final String PART_OF = "partOf";
  public static final String CONTAINS = "contains";
  private final Integer minLines;
  private final Integer maxLines;

  public SalesQuotationLine(Integer minLines, Integer maxLines) {
    this.minLines = minLines;
    this.maxLines = maxLines;
  }

  @Override
  public void flatMap(TransactionalDataObject quotation,
    Collector<TransactionalDataObject> collector) throws Exception {

    Random random = new Random();

    for(int i = 0; i <= minLines + random.nextInt(maxLines - minLines); i++ ) {

      TransactionalDataObject salesQuotationLine =
        new TransactionalDataObject();

      salesQuotationLine.setCaseId(quotation.getCaseId());
      salesQuotationLine.getReferences().put(PART_OF, quotation.getId());

      collector.collect(salesQuotationLine);
    }

  }
}
