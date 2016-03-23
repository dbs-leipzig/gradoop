package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.datagen.GraphGenerator;
import org.gradoop.model.impl.LogicalGraph;

import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.generator.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generator.VendorGenerator;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.util.GradoopFlinkConfig;


public class FoodBroker
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GraphGenerator<G, V, E> {

  private final GradoopFlinkConfig<G, V, E> gradoopFlinkConfig;
  private final FoodBrokerConfig foodBrokerConfig;
  protected final ExecutionEnvironment env;

  public FoodBroker(ExecutionEnvironment env,
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
    this.env = env;
  }

  @Override
  public LogicalGraph<G, V, E> generate(Integer scaleFactor) {

    foodBrokerConfig.setScaleFactor(scaleFactor);

    DataSet<MasterDataObject<V>> customers = new CustomerGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    DataSet<MasterDataObject<V>> vendors = new VendorGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    DataSet<MasterDataObject<V>> logistics = new LogisticsGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    DataSet<MasterDataObject<V>> employees = new EmployeeGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    DataSet<MasterDataObject<V>> products = new ProductGenerator<>(
      env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory()).generate();

    DataSet<MasterDataObject<V>> vertices = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products);

    try {
      vertices.print();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return LogicalGraph.createEmptyGraph(gradoopFlinkConfig);
  }
}
