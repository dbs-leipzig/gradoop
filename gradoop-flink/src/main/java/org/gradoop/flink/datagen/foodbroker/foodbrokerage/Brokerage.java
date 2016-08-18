package org.gradoop.flink.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.AbstractBusinessProcess;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Created by Stephan on 18.08.16.
 */
public class Brokerage extends AbstractBusinessProcess {

  public Brokerage(FoodBrokerConfig foodBrokerConfig,
    GradoopFlinkConfig gradoopFlinkConfig, ExecutionEnvironment env,
    DataSet<Vertex> customers, DataSet<Vertex> vendors,
    DataSet<Vertex> logistics, DataSet<Vertex> employees,
    DataSet<Vertex> products, DataSet<Long> caseSeeds) {
    super(foodBrokerConfig, gradoopFlinkConfig, env, customers, vendors,
      logistics, employees, products, caseSeeds);
  }

  @Override
  public void execute() {
    FoodBrokerage foodBrokerage = new FoodBrokerage(
      gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(),
      gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);


    DataSet<Tuple2<GraphTransaction, FoodBrokerMaps>> foodBrokerageTuple =
      caseSeeds
        .mapPartition(foodBrokerage)
        .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
        .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
        .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
        .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
        .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
        .withBroadcastSet(productPriceDataMap, Constants.PRODUCT_PRICE_MAP)
      ;

    graphTransactions = foodBrokerageTuple
      .map(new MapFunction<Tuple2<GraphTransaction, FoodBrokerMaps>, GraphTransaction>() {
        @Override
        public GraphTransaction map(
          Tuple2<GraphTransaction, FoodBrokerMaps>
            tuple) throws
          Exception {
          return tuple.f0;
        }
      });


  }
}
