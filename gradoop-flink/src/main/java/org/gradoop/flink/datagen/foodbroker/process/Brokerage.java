package org.gradoop.flink.datagen.foodbroker.process;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.functions.TransactionFromTuple;
import org.gradoop.flink.datagen.foodbroker.functions.FoodBrokerageTuple;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class Brokerage extends AbstractBusinessProcess {

  private DataSet<Tuple2<GraphTransaction, FoodBrokerMaps>> foodBrokerageTuple;

  public Brokerage(FoodBrokerConfig foodBrokerConfig,
    GradoopFlinkConfig gradoopFlinkConfig,
    DataSet<Vertex> customers, DataSet<Vertex> vendors,
    DataSet<Vertex> logistics, DataSet<Vertex> employees,
    DataSet<Vertex> products, DataSet<Long> caseSeeds) {
    super(foodBrokerConfig, gradoopFlinkConfig, customers, vendors,
      logistics, employees, products, caseSeeds);
  }

  public DataSet<Tuple2<GraphTransaction, FoodBrokerMaps>> getTuple() {
    return foodBrokerageTuple;
  }

  @Override
  public void execute() {
    FoodBrokerageTuple foodBrokerageTuple = new FoodBrokerageTuple(
      gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(),
      gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);

    this.foodBrokerageTuple = caseSeeds
      .mapPartition(foodBrokerageTuple)
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
      .withBroadcastSet(productPriceDataMap, Constants.PRODUCT_PRICE_MAP);

    graphTransactions = this.foodBrokerageTuple
      .map(new TransactionFromTuple<FoodBrokerMaps>());
  }
}
