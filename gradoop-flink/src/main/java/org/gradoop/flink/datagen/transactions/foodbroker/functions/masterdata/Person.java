
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a person vertex.
 */
public abstract class Person extends MasterData {
  /**
   * List of possible cities.
   */
  private List<String> cities;
  /**
   * Amount of pissible cities.
   */
  private Integer cityCount;
  /**
   * EPGM vertex factory.
   */
  private final VertexFactory vertexFactory;
  /**
   * FoodBroker configuration.
   */
  private FoodBrokerConfig foodBrokerConfig;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory
   * @param foodBrokerConfig FoodBroker configuration
   */
  public Person(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    this.vertexFactory = vertexFactory;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcasted lists
    cities = getRuntimeContext().getBroadcastVariable(Constants.CITIES_BC);
    //get the size
    cityCount = cities.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    Properties properties = createDefaultProperties(seed, getAcronym());
    Random random = new Random();
    //set rnd location
    String[] location = cities.get(random.nextInt(cityCount)).split("-");
    properties.set(Constants.CITY_KEY, location[0]);
    properties.set(Constants.STATE_KEY, location[1]);
    properties.set(Constants.COUNTRY_KEY, location[2]);

    return vertexFactory.createVertex(getClassName(), properties);
  }

  /**
   * Returns the FoodBroker configuration.
   *
   * @return FoodBroker configuration
   */
  public FoodBrokerConfig getFoodBrokerConfig() {
    return foodBrokerConfig;
  }
}
