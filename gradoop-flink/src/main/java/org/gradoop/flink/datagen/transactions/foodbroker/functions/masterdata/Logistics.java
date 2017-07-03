
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a logistic vertex.
 */
public class Logistics extends MasterData {
  /**
   * List of possible adjectives.
   */
  private List<String> adjectives;
  /**
   * List of possible nouns.
   */
  private List<String> nouns;
  /**
   * List of possible cities.
   */
  private List<String> cities;
  /**
   * Amount of possible adjectives.
   */
  private Integer adjectiveCount;
  /**
   * Amount of possible nouns.
   */
  private Integer nounCount;
  /**
   * Amount of pissible cities.
   */
  private Integer cityCount;
  /**
   * EPGM vertex factory.
   */
  private final VertexFactory vertexFactory;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory
   */
  public Logistics(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcast lists
    adjectives = getRuntimeContext().getBroadcastVariable(Constants.ADJECTIVES_BC);
    nouns = getRuntimeContext().getBroadcastVariable(Constants.NOUNS_BC);
    cities = getRuntimeContext().getBroadcastVariable(Constants.CITIES_BC);
    //get their sizes
    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
    cityCount = cities.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    Properties properties = createDefaultProperties(seed, getAcronym());
    Random random = new Random();
    //set rnd city and name
    String[] location = cities.get(random.nextInt(cityCount)).split("-");
    properties.set(Constants.CITY_KEY, location[0]);
    properties.set(Constants.STATE_KEY, location[1]);
    properties.set(Constants.COUNTRY_KEY, location[2]);

    properties.set(Constants.NAME_KEY, adjectives.get(random.nextInt(adjectiveCount)) + " " +
      nouns.get(random.nextInt(nounCount)));
    return vertexFactory.createVertex(getClassName(), properties);
  }

  @Override
  public String getAcronym() {
    return Constants.LOGISTICS_ACRONYM;
  }

  @Override
  public String getClassName() {
    return Constants.LOGISTICS_VERTEX_LABEL;
  }
}
