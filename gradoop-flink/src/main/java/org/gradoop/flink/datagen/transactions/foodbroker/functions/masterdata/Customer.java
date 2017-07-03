
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a customer vertex.
 */
public class Customer extends BusinessRelation {
  /**
   * List of possible adjectives.
   */
  private List<String> adjectives;
  /**
   * List of possible nouns.
   */
  private List<String> nouns;
  /**
   * Amount of possible adjectives.
   */
  private Integer adjectiveCount;
  /**
   * Amount of possible nouns.
   */
  private Integer nounCount;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory
   * @param foodBrokerConfig FoodBroker configuration.
   */
  public Customer(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    super(vertexFactory, foodBrokerConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcasted lists
    adjectives = getRuntimeContext().getBroadcastVariable(Constants.ADJECTIVES_BC);
    nouns = getRuntimeContext().getBroadcastVariable(Constants.NOUNS_BC);
    //get their sizes
    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //set rnd name
    Random random = new Random();
    Vertex vertex = super.map(seed);
    vertex.setProperty(Constants.NAME_KEY, adjectives.get(random.nextInt(adjectiveCount)) + " " +
      nouns.get(random.nextInt(nounCount)));
    return vertex;
  }

  @Override
  public String getAcronym() {
    return Constants.CUSTOMER_ACRONYM;
  }

  @Override
  public String getClassName() {
    return Constants.CUSTOMER_VERTEX_LABEL;
  }
}
