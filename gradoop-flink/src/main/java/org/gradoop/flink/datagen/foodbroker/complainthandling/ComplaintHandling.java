package org.gradoop.flink.datagen.foodbroker.complainthandling;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.AbstractBusinessProcess;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
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
    GradoopFlinkConfig gradoopFlinkConfig, ExecutionEnvironment env,
    DataSet<Vertex> customers, DataSet<Vertex> vendors,
    DataSet<Vertex> logistics, DataSet<Vertex> employees,
    DataSet<Vertex> products, DataSet<Long> caseSeeds,
    DataSet<Tuple2<GraphTransaction, FoodBrokerMaps>> foodBrokerageTuple) {
    super(foodBrokerConfig, gradoopFlinkConfig, env, customers, vendors,
      logistics, employees, products, caseSeeds);
    this.foodBrokerageTuple = foodBrokerageTuple;
  }

  private DataSet<Vertex> getNewMasterData() {
    return userClients;
  }

  @Override
  public void execute() {
    DataSet<Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>>
      deliveryNotes =
      foodBrokerageTuple
        .map(
          new MapFunction<Tuple2<GraphTransaction, FoodBrokerMaps>, Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>>() {
            @Override
            public Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>> map(
              Tuple2<GraphTransaction, FoodBrokerMaps>
                tuple) throws
              Exception {
              Set<Vertex> deliveryNotes = Sets.newHashSet();
              for (Vertex vertex : tuple.f0.getVertices()) {
                if (vertex.getLabel().equals("DeliveryNote")){
                  deliveryNotes.add(vertex);
                }
              }
              Set<Edge> salesOrderLines = Sets.newHashSet();
              Set<Edge> purchOrderLines = Sets.newHashSet();
              for (Edge edge : tuple.f0.getEdges()) {
                if (edge.getLabel().equals("SalesOrderLine")) {
                  salesOrderLines.add(edge);
                } else if (edge.getLabel().equals("PurchOrderLine")) {
                  purchOrderLines.add(edge);
                }
              }
              return new Tuple4<>(deliveryNotes, tuple.f1, salesOrderLines, purchOrderLines);
            }
          });


    DataSet<Tuple2<GraphTransaction, Set<Vertex>>> complaintHandlingTuple =
      deliveryNotes
        .mapPartition(new NewComplaintHandling(
          gradoopFlinkConfig.getGraphHeadFactory(),
          gradoopFlinkConfig.getVertexFactory(),
          gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig
        ))
        .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
        .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
        .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
        .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
        .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
        .withBroadcastSet(employees, Employee.CLASS_NAME)
        .withBroadcastSet(customers, Customer.CLASS_NAME);

    graphTransactions = complaintHandlingTuple
      .map(new MapFunction<Tuple2<GraphTransaction, Set<Vertex>>, GraphTransaction>() {
        @Override
        public GraphTransaction map(
          Tuple2<GraphTransaction, Set<Vertex>> tuple)
          throws
          Exception {
          return tuple.f0;
        }
      });

    userClients = complaintHandlingTuple
      .flatMap(new FlatMapFunction<Tuple2<GraphTransaction, Set<Vertex>>, Vertex>() {
        @Override
        public void flatMap(
          Tuple2<GraphTransaction, Set<Vertex>> tuple,
          Collector<Vertex> collector) throws Exception {
          for (Vertex vertex : tuple.f1) {
            collector.collect(vertex);
          }
        }
      });
  }
}