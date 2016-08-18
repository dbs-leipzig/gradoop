package org.gradoop.flink.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

/**
 * Created by Stephan on 18.08.16.
 */
public interface BusinessProcess {

  void execute();

  DataSet<GraphTransaction> getTransactions();
}
