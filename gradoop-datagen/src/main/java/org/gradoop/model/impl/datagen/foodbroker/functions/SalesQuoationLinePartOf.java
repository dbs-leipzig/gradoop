package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotationLine;

import java.util.Random;

/**
 * Created by peet on 24.03.16.
 */
public class SalesQuoationLinePartOf implements
  FlatMapFunction<SalesQuotation, SalesQuotationLine> {

  private final Integer productCount;
  private final Integer minLines;
  private final Integer maxLines;

  public SalesQuoationLinePartOf(Integer productCount, Integer minLines,
    Integer maxLines) {
    this.productCount = productCount;
    this.minLines = minLines;
    this.maxLines = maxLines;
  }


  @Override
  public void flatMap(SalesQuotation salesQuotation,
    Collector<SalesQuotationLine> collector) throws Exception {

    Random random = new Random();

    for(int i = 0; i <= minLines + random.nextInt(maxLines - minLines); i++ ) {

      SalesQuotationLine salesQuotationLine = new SalesQuotationLine();

      salesQuotationLine.setPartOf(salesQuotation.getId());
      salesQuotationLine.setCountains((long) random.nextInt(productCount));

      collector.collect(salesQuotationLine);
    }
  }
}
