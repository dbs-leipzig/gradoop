package org.gradoop.flink.datagen.foodbroker.process;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
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

import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ComplaintHandlingOld extends AbstractBusinessProcess {

  private DataSet<Vertex> userClients;

  public ComplaintHandlingOld(FoodBrokerConfig foodBrokerConfig,
    GradoopFlinkConfig gradoopFlinkConfig,
    DataSet<Vertex> customers, DataSet<Vertex> vendors,
    DataSet<Vertex> logistics, DataSet<Vertex> employees,
    DataSet<Vertex> products, DataSet<Long> caseSeeds,
    DataSet<GraphTransaction> brokerageTransactions) {
    super(foodBrokerConfig, gradoopFlinkConfig, customers, vendors,
      logistics, employees, products, caseSeeds);
    this.foodBrokerageTuple = foodBrokerageTuple;
  }

  public DataSet<Vertex> getNewMasterData() {
    return userClients;
  }

  @Override
  public void execute() {

    DataSet<FoodBrokerMaps> maps = foodBrokerageTuple
      .map(
        new MapFunction<Tuple2<GraphTransaction, FoodBrokerMaps>, FoodBrokerMaps>() {
          @Override
          public FoodBrokerMaps map(
            Tuple2<GraphTransaction, FoodBrokerMaps> tuple) throws Exception {
            return tuple.f1;
          }
        })
      .flatMap(new FlatMapFunction<FoodBrokerMaps, FoodBrokerMaps>() {
        @Override
        public void flatMap(FoodBrokerMaps foodBrokerMaps,
          Collector<FoodBrokerMaps> collector) throws Exception {
          Map<GradoopId, Vertex> vertices = Maps.newHashMap();
          Map<Tuple2<String, GradoopId>, Set<Edge>> edges = Maps.newHashMap();
          for (Map.Entry<GradoopId, Vertex> entry : foodBrokerMaps.getVertexMap().entrySet()) {
            if (entry.getValue().getLabel().equals("SalesOrder") ||
                  entry.getValue().getLabel().equals("DeliveryNote") ) {
              vertices.put(entry.getKey(), entry.getValue());
            }
          }
          for (Map.Entry<Tuple2<String, GradoopId>, Set<Edge>> entry :
            foodBrokerMaps.getEdgeMap().entrySet()) {
            switch (entry.getKey().f0) {
              case "contains" :
              case "receives" :
              case "operatedBy" :
              case "placedAt" :
              case "receivedFrom" :
              case "SalesOrderLine" :
              case "PurchOrderLine" :
                edges.put(entry.getKey(), entry.getValue());
                break;
              default:
                break;
            }
          }
          if (!vertices.isEmpty() && !edges.isEmpty()) {
            collector.collect(new FoodBrokerMaps(vertices, edges));
          }
        }
      });

    long globalSeed = 0;
    try {
      globalSeed = caseSeeds.count() + 1;
    } catch (Exception e) {
      e.printStackTrace();
    }

    DataSet<Tuple2<GraphTransaction, Set<Vertex>>> complaintHandlingTuple =
      maps
        .mapPartition(new ComplaintTuple(
          gradoopFlinkConfig.getGraphHeadFactory(),
          gradoopFlinkConfig.getVertexFactory(),
          gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig, globalSeed))
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