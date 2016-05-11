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

public class Employee<V extends EPGMVertex>
  extends RichMapFunction<MasterDataSeed, V> {

  public static final String CLASS_NAME = "Employee";
  public static final String FIRST_NAMES_MALE_BC = "firstNamesMale";
  public static final String FIRST_NAMES_FEMALE_BC = "firstNamesFemale";
  public static final String LAST_NAMES_BC = "nouns";
  public static final String CITIES_BC = "cities";  
  
  private List<String> firstNamesFemale;
  private List<String> firstNamesMale;
  private List<String> lastNames;
  private List<String> cities;
  private Integer firstNameCountFemale;
  private Integer firstNameCountMale;
  private Integer lastNameCount;
  private Integer cityCount;

  private final EPGMVertexFactory<V> vertexFactory;

  public Employee(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    firstNamesFemale = getRuntimeContext()
      .getBroadcastVariable(FIRST_NAMES_FEMALE_BC);
    firstNamesMale = getRuntimeContext()
      .getBroadcastVariable(FIRST_NAMES_MALE_BC);
    lastNames = getRuntimeContext()
      .getBroadcastVariable(LAST_NAMES_BC);
    cities = getRuntimeContext()
      .getBroadcastVariable(CITIES_BC);

    firstNameCountFemale = firstNamesFemale.size();
    firstNameCountMale = firstNamesMale.size();
    lastNameCount = lastNames.size();
    cityCount = cities.size();
  }

  @Override
  public V map(MasterDataSeed seed) throws  Exception {

    Random random = new Random();

    String city = cities.get(random.nextInt(cityCount));
    String gender = null;
    String name = null;

    if(seed.hashCode() % 2 == 0) {
      gender = "f";
      name = firstNamesFemale.get(random.nextInt(firstNameCountFemale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    } else {
      gender = "m";
      name = firstNamesMale.get(random.nextInt(firstNameCountMale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    }

    String bid = "EMP" + seed.getId().toString();

    PropertyList properties = new PropertyList();

    properties.set("city", city);
    properties.set("name", name);
    properties.set("num", bid);
    properties.set("gender", gender);

    properties.set(MasterDataSeed.QUALITY, seed.getQuality());

    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);

    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    return vertexFactory.createVertex(Customer.CLASS_NAME, properties);
  }
}
