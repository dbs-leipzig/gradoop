/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.flink.datagen.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

public class Employee
  extends RichMapFunction<MasterDataSeed, Vertex> {

  public static final String CLASS_NAME = "Employee";
  public static final String FIRST_NAMES_MALE_BC = "firstNamesMale";
  public static final String FIRST_NAMES_FEMALE_BC = "firstNamesFemale";
  public static final String LAST_NAMES_BC = "nouns";
  public static final String CITIES_BC = "cities";
  private static String ACRONYM = "EMP";

  private List<String> firstNamesFemale;
  private List<String> firstNamesMale;
  private List<String> lastNames;
  private List<String> cities;
  private Integer firstNameCountFemale;
  private Integer firstNameCountMale;
  private Integer lastNameCount;
  private Integer cityCount;

  private final VertexFactory vertexFactory;

  public Employee(VertexFactory vertexFactory) {
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
  public Vertex map(MasterDataSeed seed) throws  Exception {
    PropertyList properties = MasterData.createDefaultProperties(ACRONYM, seed);

    Random random = new Random();

    properties.set("city", cities.get(random.nextInt(cityCount)));

    String gender;
    String name;

    if(seed.getNumber() % 2 == 0) {
      gender = "f";
      name = firstNamesFemale.get(random.nextInt(firstNameCountFemale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    } else {
      gender = "m";
      name = firstNamesMale.get(random.nextInt(firstNameCountMale)) +
        " " + lastNames.get(random.nextInt(lastNameCount));
    }

    properties.set("name", name);
    properties.set("gender", gender);

    return vertexFactory.createVertex(Employee.CLASS_NAME, properties);
  }
}
