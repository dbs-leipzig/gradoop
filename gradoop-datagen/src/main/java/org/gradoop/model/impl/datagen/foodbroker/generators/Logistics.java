package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Logistics<V extends EPGMVertex>
  extends RichMapFunction<MasterDataSeed, V> {

  private static final String CLASS_NAME = "Logistics";
  private List<String> adjectives;
  private List<String> nouns;
  private List<String> cities;
  private Integer adjectiveCount;
  private Integer nounCount;
  private Integer cityCount;

  private final EPGMVertexFactory<V> vertexFactory;

  public Logistics(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    adjectives = getRuntimeContext()
      .getBroadcastVariable(LogisticsGenerator.ADJECTIVES_BC);
    nouns = getRuntimeContext()
      .getBroadcastVariable(LogisticsGenerator.NOUNS_BC);
    cities = getRuntimeContext()
      .getBroadcastVariable(LogisticsGenerator.CITIES_BC);

    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
    cityCount = cities.size();
  }

  @Override
  public V map(MasterDataSeed seed) throws  Exception {

    Random random = new Random();

    String city = cities.get(random.nextInt(cityCount));
    String name = adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nouns.get(random.nextInt(nounCount));

    String bid = "LOG" + seed.getId().toString();

    PropertyList properties = new PropertyList();

    properties.set("city", city);
    properties.set("name", name);
    properties.set("num", bid);

    properties.set(MasterDataSeed.QUALITY, seed.getQuality());

    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);

    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    return vertexFactory.createVertex(Customer.CLASS_NAME, properties);
  }
}
