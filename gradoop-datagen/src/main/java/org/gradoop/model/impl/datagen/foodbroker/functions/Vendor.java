package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.datagen.foodbroker.generator.VendorGenerator;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Vendor<V extends EPGMVertex> extends
  RichMapFunction<MasterDataSeed, MasterDataObject<V>>
  implements ResultTypeQueryable<MasterDataObject<V>> {

  private final EPGMVertexFactory<V> vertexFactory;
  private List<String> adjectives;
  private List<String> nouns;
  private List<String> cities;
  private Integer adjectiveCount;
  private Integer nounCount;
  private Integer cityCount;

  public Vendor(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    adjectives = getRuntimeContext()
      .getBroadcastVariable(VendorGenerator.ADJECTIVES_BC);
    nouns = getRuntimeContext()
      .getBroadcastVariable(VendorGenerator.NOUNS_BC);
    cities = getRuntimeContext()
      .getBroadcastVariable(VendorGenerator.CITIES_BC);

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

    String bid = "VEN" + seed.getLongId().toString();

    PropertyList properties = new PropertyList();

    properties.set("city", city);
    properties.set("name", name);
    properties.set("num", bid);

    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);

    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    V vertex = vertexFactory
      .createVertex(VendorGenerator.CLASS_NAME, properties);

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
