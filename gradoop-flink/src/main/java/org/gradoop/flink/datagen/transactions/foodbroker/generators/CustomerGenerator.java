
package org.gradoop.flink.datagen.transactions.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.Customer;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Generator for vertices which represent customers.
 */
public class CustomerGenerator extends AbstractMasterDataGenerator {

  /**
   * Valued constructor.
   *
   * @param gradoopFlinkConfig Gradoop Flink configuration.
   * @param foodBrokerConfig FoodBroker configuration.
   */
  public CustomerGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  @Override
  public DataSet<Vertex> generate() {
    List<MasterDataSeed> seeds = getMasterDataSeeds(Constants.CUSTOMER_VERTEX_LABEL);
    List<String> cities = foodBrokerConfig
      .getStringValuesFromFile("cities");
    List<String> companies = foodBrokerConfig
      .getStringValuesFromFile("companies");
    List<String> holdings = foodBrokerConfig
      .getStringValuesFromFile("holdings");
    holdings.add(Constants.HOLDING_TYPE_PRIVATE);
    List<String> adjectives = foodBrokerConfig
      .getStringValuesFromFile("customer.adjectives");
    List<String> nouns = foodBrokerConfig
      .getStringValuesFromFile("customer.nouns");

    return env.fromCollection(seeds)
      .map(new Customer(vertexFactory, foodBrokerConfig))
      .withBroadcastSet(env.fromCollection(adjectives), Constants.ADJECTIVES_BC)
      .withBroadcastSet(env.fromCollection(nouns), Constants.NOUNS_BC)
      .withBroadcastSet(env.fromCollection(cities), Constants.CITIES_BC)
      .withBroadcastSet(env.fromCollection(companies), Constants.COMPANIES_BC)
      .withBroadcastSet(env.fromCollection(holdings), Constants.HOLDINGS_BC)
      .returns(vertexFactory.getType());
  }
}
