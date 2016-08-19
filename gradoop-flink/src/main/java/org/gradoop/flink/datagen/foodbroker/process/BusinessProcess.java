package org.gradoop.flink.datagen.foodbroker.process;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

public interface BusinessProcess {

  void execute();

  DataSet<GraphTransaction> getTransactions();
}
