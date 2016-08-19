package org.gradoop.flink.datagen.foodbroker.process;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.functions.TransactionFromTuple;
import org.gradoop.flink.datagen.foodbroker.functions.ComplaintData;
import org.gradoop.flink.datagen.foodbroker.functions.ComplaintTuple;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.functions.UserClients;
import org.gradoop.flink.datagen.foodbroker.masterdata.Customer;
import org.gradoop.flink.datagen.foodbroker.masterdata.Employee;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Set;

/**
 *
 */
public class ComplaintHandling extends AbstractBusinessProcess {

  private DataSet<Vertex> userClients;

  private DataSet<Tuple2<GraphTransaction, FoodBrokerMaps>> foodBrokerageTuple;


  public ComplaintHandling(FoodBrokerConfig foodBrokerConfig,
    GradoopFlinkConfig gradoopFlinkConfig,
    DataSet<Vertex> customers, DataSet<Vertex> vendors,
    DataSet<Vertex> logistics, DataSet<Vertex> employees,
    DataSet<Vertex> products, DataSet<Long> caseSeeds,
    DataSet<Tuple2<GraphTransaction, FoodBrokerMaps>> foodBrokerageTuple) {
    super(foodBrokerConfig, gradoopFlinkConfig, customers, vendors,
      logistics, employees, products, caseSeeds);
    this.foodBrokerageTuple = foodBrokerageTuple;
  }

  public DataSet<Vertex> getNewMasterData() {
    return userClients;
  }

  @Override
  public void execute() {
    DataSet<Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>>
      deliveryNotes = foodBrokerageTuple
        .map(new ComplaintData());

    DataSet<Tuple2<GraphTransaction, Set<Vertex>>> complaintHandlingTuple =
      deliveryNotes
        .mapPartition(new ComplaintTuple(
          gradoopFlinkConfig.getGraphHeadFactory(),
          gradoopFlinkConfig.getVertexFactory(),
          gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig))
        .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
        .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
        .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
        .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
        .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
        .withBroadcastSet(employees, Employee.CLASS_NAME)
        .withBroadcastSet(customers, Customer.CLASS_NAME);

    graphTransactions = complaintHandlingTuple
      .map(new TransactionFromTuple<Set<Vertex>>());

    userClients = complaintHandlingTuple
      .flatMap(new UserClients());
  }
}