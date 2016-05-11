package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Product<V extends EPGMVertex>
  extends RichMapFunction<MasterDataSeed, V> {

  public static final String CLASS_NAME = "Product";
  public static final String NAMES_GROUPS_BC = "nameGroupPairs";
  public static final String ADJECTIVES_BC = "adjectives";

  private List<Tuple2<String, String>> nameGroupPairs;
  private List<String> adjectives;

  private Integer nameGroupPairCount;
  private Integer adjectiveCount;

  private final EPGMVertexFactory<V> vertexFactory;

  public Product(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    nameGroupPairs = getRuntimeContext()
      .getBroadcastVariable(NAMES_GROUPS_BC);
    adjectives = getRuntimeContext()
      .getBroadcastVariable(ADJECTIVES_BC);

    nameGroupPairCount = nameGroupPairs.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public V map(MasterDataSeed seed) throws  Exception {

    Random random = new Random();

    Tuple2<String, String> nameGroupPair = nameGroupPairs
      .get(random.nextInt(nameGroupPairCount));

    String bid = "PRD" + seed.getId().toString();
    String name = adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nameGroupPair.f0;
    String category = nameGroupPair.f1;

    PropertyList properties = new PropertyList();

    properties.set("name", name);
    properties.set("category", category);

    properties.set(MasterDataSeed.QUALITY, seed.getQuality());

    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);

    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    return vertexFactory.createVertex(Customer.CLASS_NAME, properties);
  }
}
