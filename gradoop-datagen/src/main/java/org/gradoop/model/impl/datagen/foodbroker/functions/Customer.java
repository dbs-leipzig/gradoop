package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Customer extends
  RichMapFunction<MasterDataSeed, MasterDataObject> {

  public static final String CLASS_NAME = "Customer";
  public static final String ADJECTIVES_BC = "adjectives";
  public static final String NOUNS_BC = "nouns";
  public static final String CITIES_BC = "cities";
  
  private List<String> adjectives;
  private List<String> nouns;
  private List<String> cities;
  private Integer adjectiveCount;
  private Integer nounCount;
  private Integer cityCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    adjectives = getRuntimeContext()
      .getBroadcastVariable(ADJECTIVES_BC);
    nouns = getRuntimeContext()
      .getBroadcastVariable(NOUNS_BC);
    cities = getRuntimeContext()
      .getBroadcastVariable(CITIES_BC);

    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
    cityCount = cities.size();
  }

  @Override
  public MasterDataObject map(MasterDataSeed seed) throws  Exception {

    Random random = new Random();

    String city = cities.get(random.nextInt(cityCount));
    String name = adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nouns.get(random.nextInt(nounCount));

    String bid = "CUS" + seed.getLongId().toString();

    PropertyList properties = new PropertyList();

    properties.set("city", city);
    properties.set("name", name);
    properties.set("num", bid);

    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);

    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    return  new MasterDataObject(seed, Customer.CLASS_NAME, properties);
  }
}
