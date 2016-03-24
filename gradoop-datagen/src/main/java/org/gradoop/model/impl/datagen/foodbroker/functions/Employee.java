package org.gradoop.model.impl.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.datagen.foodbroker.generator.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Employee<V extends EPGMVertex> extends
  RichMapFunction<MasterDataSeed, MasterDataObject> {

  private static final String CLASS_NAME = "Employee";
  private List<String> firstNamesFemale;
  private List<String> firstNamesMale;
  private List<String> lastNames;
  private List<String> cities;
  private Integer firstNameCountFemale;
  private Integer firstNameCountMale;
  private Integer lastNameCount;
  private Integer cityCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    firstNamesFemale = getRuntimeContext()
      .getBroadcastVariable(EmployeeGenerator.FIRST_NAMES_FEMALE_BC);
    firstNamesMale = getRuntimeContext()
      .getBroadcastVariable(EmployeeGenerator.FIRST_NAMES_MALE_BC);
    lastNames = getRuntimeContext()
      .getBroadcastVariable(EmployeeGenerator.LAST_NAMES_BC);
    cities = getRuntimeContext()
      .getBroadcastVariable(EmployeeGenerator.CITIES_BC);

    firstNameCountFemale = firstNamesFemale.size();
    firstNameCountMale = firstNamesMale.size();
    lastNameCount = lastNames.size();
    cityCount = cities.size();
  }

  @Override
  public MasterDataObject map(MasterDataSeed seed) throws  Exception {

    Random random = new Random();

    String city = cities.get(random.nextInt(cityCount));
    String gender = null;
    String name = null;

    if(seed.getLongId() % 2 == 0) {
      gender = "f";
      name = firstNamesFemale.get(random.nextInt(firstNameCountFemale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    } else {
      gender = "m";
      name = firstNamesMale.get(random.nextInt(firstNameCountMale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    }

    String bid = "EMP" + seed.getLongId().toString();

    PropertyList properties = new PropertyList();

    properties.set("city", city);
    properties.set("name", name);
    properties.set("num", bid);
    properties.set("gender", gender);

    properties.set(BusinessTransactionGraphs.SUPERTYPE_KEY,
      BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER);

    properties.set(BusinessTransactionGraphs.SOURCEID_KEY, "ERP_" + bid);

    return new MasterDataObject(seed, Employee.CLASS_NAME, properties);
  }
}
