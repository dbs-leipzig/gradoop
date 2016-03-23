package org.gradoop.model.impl.datagen.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.masterdata.CustomerGenerator;
import org.gradoop.model.impl.datagen.model.MasterDataObject;
import org.gradoop.model.impl.datagen.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Customer<V extends EPGMVertex> extends
  RichMapFunction<MasterDataSeed, MasterDataObject<V>>
  implements ResultTypeQueryable<MasterDataObject<V>> {

  private final EPGMVertexFactory<V> vertexFactory;
  private List<String> adjectives;
  private List<String> nouns;
  private List<String> cities;
  private Integer adjectiveCount;
  private Integer nounCount;
  private Integer cityCount;

  public Customer(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    adjectives = getRuntimeContext()
      .getBroadcastVariable(CustomerGenerator.ADJECTIVES_BC);
    nouns = getRuntimeContext()
      .getBroadcastVariable(CustomerGenerator.NOUNS_BC);
    cities = getRuntimeContext()
      .getBroadcastVariable(CustomerGenerator.CITIES_BC);

    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
    cityCount = cities.size();
  }

  @Override
  public MasterDataObject<V> map(MasterDataSeed seed) throws  Exception {

    Random random = new Random();

    String city = cities.get(random.nextInt(cityCount));
    String name = adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nouns.get(random.nextInt(nounCount));

    PropertyList properties = new PropertyList();

    properties.set("city", city);
    properties.set("name", name);
    properties.set("_superType", "MasterData");

    V vertex = vertexFactory
      .createVertex(CustomerGenerator.CLASS_NAME, properties);

    return new MasterDataObject<>(seed, vertex);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TypeInformation<MasterDataObject<V>> getProducedType() {
    return new TupleTypeInfo<>(
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.SHORT_TYPE_INFO,
      TypeExtractor.createTypeInfo(vertexFactory.getType()));
  }
}
