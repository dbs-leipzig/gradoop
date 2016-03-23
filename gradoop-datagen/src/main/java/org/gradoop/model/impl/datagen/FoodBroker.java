package org.gradoop.model.impl.datagen;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.datagen.GraphGenerator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.datagen.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.masterdata.CustomerGenerator;
import org.gradoop.model.impl.datagen.model.MasterDataObject;
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

    CustomerGenerator<V> customerGenerator =
      new CustomerGenerator<>(
        env, foodBrokerConfig, gradoopFlinkConfig.getVertexFactory());

    DataSet<MasterDataObject<V>> customers = customerGenerator.generate();

    try {
      customers.print();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return LogicalGraph.createEmptyGraph(gradoopFlinkConfig);
  }
}
