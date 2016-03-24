package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.model.impl.datagen.foodbroker.model.TransactionalDataObject;

import java.util.Random;

/**
 * Created by peet on 24.03.16.
 */
public class RandomForeignKey implements
  MapFunction<TransactionalDataObject, TransactionalDataObject> {

  private final Integer employeeCount;

  public RandomForeignKey(Integer employeeCount) {
    this.employeeCount = employeeCount;
  }

  @Override
  public TransactionalDataObject map(
    TransactionalDataObject transactionalDataObject) throws Exception {

    transactionalDataObject
      .setForeignKey((long)new Random().nextInt(employeeCount));

    return transactionalDataObject;
  }
}
