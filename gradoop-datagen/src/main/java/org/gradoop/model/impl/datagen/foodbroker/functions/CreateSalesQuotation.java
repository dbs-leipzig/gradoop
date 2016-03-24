package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.SalesQuotation;

import java.util.Random;


public class CreateSalesQuotation implements MapFunction<Long, SalesQuotation> {

  private final Integer employeeCount;
  private final Integer customerCount;

  public CreateSalesQuotation(Integer employeeCount, Integer customerCount) {
    this.employeeCount = employeeCount;
    this.customerCount = customerCount;
  }

  @Override
  public SalesQuotation map(Long caseId) throws Exception {
    SalesQuotation salesQuotation = new SalesQuotation(caseId);

    Random random = new Random();
    salesQuotation.setSentBy((long) random.nextInt(employeeCount));
    salesQuotation.setSentTo((long) random.nextInt(customerCount));

    return salesQuotation;
  }
}
