package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.generators.CaseGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generators.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generators.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generators.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generators.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.generators.VendorGenerator;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;


public class FoodBroker
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {

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
  public GraphCollection<G, V, E> execute() {

    DataSet<V> customers =
      new CustomerGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> vendors =
      new VendorGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> logistics =
      new LogisticsGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> employees =
      new EmployeeGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> products =
      new ProductGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<GradoopId> caseSeeds =
      new CaseGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    // TODO: process simulation

    DataSet<V> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products);

    try {
      masterData
        .print();
    } catch (Exception e) {
      System.out.println(e);
    }

    return GraphCollection.createEmptyCollection(gradoopFlinkConfig);  }

  @Override
  public String getName() {
    return null;
  }
}
