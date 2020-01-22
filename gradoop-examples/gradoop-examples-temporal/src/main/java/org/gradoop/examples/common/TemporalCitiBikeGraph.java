/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.common;

import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Class to provide an example graph representing a dump from the citibike rentals of NYC.
 *
 * @see <a href="https://www.citibikenyc.com/system-data">https://www.citibikenyc.com/system-data</a>
 */
public class TemporalCitiBikeGraph {

  /**
   * No need for an object of this class.
   */
  private TemporalCitiBikeGraph() {
  }

  /**
   * Returns the temporal graph instance.
   *
   * @param config the gradoop flink config
   * @return the temporal graph that represents the bike rental example
   */
  public static TemporalGraph getTemporalGraph(GradoopFlinkConfig config) {
    // create loader
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(config);

    String gdlGraph = "db [" +
      // Vertices
      "(v_trip_0:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-24 15:36:53.3930\",stoptime:\"2019-08-24 15:44:54.4570\",year_birth:\"1994\"})" +
      "(v_trip_1:trip {user_type:\"Subscriber\",gender:\"0\",starttime:\"2019-08-13 23:04:13.8730\",stoptime:\"2019-08-13 23:09:21.5980\",year_birth:\"1969\"})" +
      "(v_trip_2:trip {user_type:\"Customer\",gender:\"0\",starttime:\"2019-08-20 19:48:52.0510\",stoptime:\"2019-08-20 20:04:49.3360\",year_birth:\"1969\"})" +
      "(v_Station_5:Station {name:\"31 St & Thomson Ave\",id:\"3123\",lat:\"40.74469738\",long:\"-73.93540375\"})" +
      "(v_Station_6:Station {name:\"44 Dr & Jackson Ave\",id:\"3126\",lat:\"40.74718234\",long:\"-73.9432635\"})" +
      "(v_Station_7:Station {name:\"Bond St & Fulton St\",id:\"3232\",lat:\"40.68962188790333\",long:\"-73.98304268717766\"})" +
      "(v_Station_8:Station {name:\"8 Ave & W 31 St\",id:\"3255\",lat:\"40.7505853470215\",long:\"-73.9946848154068\"})" +
      "(v_Station_9:Station {name:\"3 St & 7 Ave\",id:\"3365\",lat:\"40.6703837\",long:\"-73.97839676\"})" +
      "(v_trip_10:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-14 07:34:35.7770\",stoptime:\"2019-08-14 07:42:59.0050\",year_birth:\"1959\"})" +
      "(v_trip_11:trip {user_type:\"Customer\",gender:\"0\",starttime:\"2019-08-10 11:57:43.4750\",stoptime:\"2019-08-10 12:08:47.6450\",year_birth:\"1969\"})" +
      "(v_Station_14:Station {name:\"W 4 St & 7 Ave S\",id:\"380\",lat:\"40.73401143\",long:\"-74.00293877\"})" +
      "(v_Station_15:Station {name:\"Greenwich Ave & Charles St\",id:\"383\",lat:\"40.735238\",long:\"-74.000271\"})" +
      "(v_Station_16:Station {name:\"30 Ave & 21 St\",id:\"3612\",lat:\"40.7703743\",long:\"-73.9286078\"})" +
      "(v_Station_17:Station {name:\"40 Ave & Crescent St\",id:\"3716\",lat:\"40.753599202005596\",long:\"-73.93795609474182\"})" +
      "(v_trip_18:trip {user_type:\"Subscriber\",gender:\"2\",starttime:\"2019-08-14 20:13:18.5480\",stoptime:\"2019-08-14 20:19:30.0820\",year_birth:\"1985\"})" +
      "(v_trip_19:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-14 08:19:00.8440\",stoptime:\"2019-08-14 08:28:24.5060\",year_birth:\"1961\"})" +
      "(v_trip_20:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-31 00:51:49.1190\",stoptime:\"2019-08-31 00:57:28.3430\",year_birth:\"1987\"})" +
      "(v_Bike_21:Bike {id:\"31449\"})" + "(v_Bike_22:Bike {id:\"38431\"})" +
      "(v_Station_23:Station {name:\"W 18 St & 6 Ave\",id:\"168\",lat:\"40.73971301\",long:\"-73.99456405\"})" +
      "(v_Station_24:Station {name:\"W 22 St & 8 Ave\",id:\"453\",lat:\"40.74475148\",long:\"-73.99915362\"})" +
      "(v_trip_25:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-04 15:26:57.9660\",stoptime:\"2019-08-04 15:28:26.0740\",year_birth:\"1968\"})" +
      "(v_trip_26:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-25 17:43:36.5120\",stoptime:\"2019-08-25 17:59:06.7110\",year_birth:\"1998\"})" +
      "(v_Bike_27:Bike {id:\"27905\"})" + "(v_Bike_28:Bike {id:\"31417\"})" +
      "(v_Station_29:Station {name:\"Lexington Ave & E 29 St\",id:\"540\",lat:\"40.74311555376486\",long:\"-73.98215353488922\"})" +
      "(v_Station_30:Station {name:\"E 33 St & 1 Ave\",id:\"3687\",lat:\"40.74322681432173\",long:\"-73.97449783980846\"})" +
      "(v_trip_31:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-23 10:54:35.2180\",stoptime:\"2019-08-23 10:59:31.9620\",year_birth:\"1993\"})" +
      "(v_trip_32:trip {user_type:\"Subscriber\",gender:\"2\",starttime:\"2019-08-10 13:36:03.6470\",stoptime:\"2019-08-10 13:51:56.6000\",year_birth:\"1961\"})" +
      "(v_trip_33:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-17 13:20:40.9130\",stoptime:\"2019-08-17 13:35:50.8090\",year_birth:\"1976\"})" +
      "(v_Station_35:Station {name:\"West St & Chambers St\",id:\"426\",lat:\"40.71754834\",long:\"-74.01322069\"})" +
      "(v_Station_36:Station {name:\"5 Ave & E 29 St\",id:\"474\",lat:\"40.7451677\",long:\"-73.98683077\"})" +
      "(v_Station_37:Station {name:\"W 33 St & 7 Ave\",id:\"492\",lat:\"40.75019995\",long:\"-73.99093085\"})" +
      "(v_Station_38:Station {name:\"E 51 St & Lexington Ave\",id:\"522\",lat:\"40.75714758\",long:\"-73.97207836\"})" +
      "(v_Station_39:Station {name:\"37 Ave & 35 St\",id:\"3561\",lat:\"40.7531106\",long:\"-73.9279917\"})" +
      "(v_Station_40:Station {name:\"Adam Clayton Powell Blvd & W 126 St\",id:\"3629\",lat:\"40.809495347779475\",long:\"-73.94776493310928\"})" +
      "(v_trip_41:trip {user_type:\"Subscriber\",gender:\"2\",starttime:\"2019-08-20 07:01:21.4800\",stoptime:\"2019-08-20 07:09:49.4070\",year_birth:\"1961\"})" +
      "(v_trip_42:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-30 14:27:05.7930\",stoptime:\"2019-08-30 14:39:45.6090\",year_birth:\"1989\"})" +
      "(v_trip_43:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-12 09:33:37.3210\",stoptime:\"2019-08-12 09:41:00.3830\",year_birth:\"1975\"})" +
      "(v_Station_46:Station {name:\"University Pl & E 14 St\",id:\"382\",lat:\"40.73492695\",long:\"-73.99200509\"})" +
      "(v_Station_47:Station {name:\"E 51 St & 1 Ave\",id:\"454\",lat:\"40.75455731\",long:\"-73.96592976\"})" +
      "(v_Station_48:Station {name:\"9 Ave & W 45 St\",id:\"479\",lat:\"40.76019252\",long:\"-73.9912551\"})" +
      "(v_Station_49:Station {name:\"Kosciuszko St & Nostrand Ave\",id:\"3056\",lat:\"40.69072549\",long:\"-73.95133465\"})" +
      "(v_Station_50:Station {name:\"1 Ave & E 68 St\",id:\"3141\",lat:\"40.76500525\",long:\"-73.95818491\"})" +
      "(v_Station_51:Station {name:\"E 53 St & 3 Ave\",id:\"3459\",lat:\"40.75763227739443\",long:\"-73.96930575370789\"})" +
      "(v_trip_52:trip {user_type:\"Subscriber\",gender:\"0\",starttime:\"2019-08-28 08:45:51.1570\",stoptime:\"2019-08-28 08:50:44.7950\",year_birth:\"1969\"})" +
      "(v_trip_53:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-27 19:15:07.1820\",stoptime:\"2019-08-27 19:23:13.4820\",year_birth:\"1969\"})" +
      "(v_Station_55:Station {name:\"Bank St & Washington St\",id:\"238\",lat:\"40.7361967\",long:\"-74.00859207\"})" +
      "(v_Station_56:Station {name:\"W 20 St & 8 Ave\",id:\"470\",lat:\"40.74345335\",long:\"-74.00004031\"})" +
      "(v_Station_57:Station {name:\"W 89 St & Columbus Ave\",id:\"3283\",lat:\"40.7882213\",long:\"-73.97041561\"})" +
      "(v_Station_58:Station {name:\"7 Ave & Park Pl\",id:\"3416\",lat:\"40.6776147\",long:\"-73.97324283\"})" +
      "(v_Station_59:Station {name:\"Vernon Blvd & 31 Ave\",id:\"3609\",lat:\"40.7692475\",long:\"-73.9354504\"})" +
      "(v_trip_60:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-25 14:19:36.7540\",stoptime:\"2019-08-25 14:33:03.5650\",year_birth:\"1952\"})" +
      "(v_trip_61:trip {user_type:\"Subscriber\",gender:\"1\",starttime:\"2019-08-06 06:45:19.9070\",stoptime:\"2019-08-06 06:54:04.0750\",year_birth:\"1980\"})" +
      "(v_Bike_62:Bike {id:\"14974\"})" + "(v_Bike_63:Bike {id:\"19577\"})" +
      "(v_Bike_64:Bike {id:\"26736\"})" + "(v_Bike_65:Bike {id:\"28414\"})" +
      "(v_Bike_66:Bike {id:\"28759\"})" + "(v_Bike_67:Bike {id:\"33301\"})" +
      "(v_Bike_68:Bike {id:\"33848\"})" + "(v_Bike_69:Bike {id:\"33962\"})" +
      "(v_Bike_3:Bike {id:\"35318\"})" + "(v_Bike_4:Bike {id:\"38654\"})" +
      "(v_Bike_44:Bike {id:\"32930\"})" + "(v_Bike_45:Bike {id:\"33867\"})" +
      "(v_Bike_34:Bike {id:\"38724\"})" + "(v_Bike_54:Bike {id:\"39117\"})" +
      "(v_Bike_12:Bike {id:\"20564\"})" + "(v_Bike_13:Bike {id:\"33592\"})" +
      "(v_Station_70:Station {name:\"Bayard St & Baxter St\",id:\"355\",lat:\"40.71602118\",long:\"-73.99974372\"})" +
      "(v_Station_71:Station {name:\"Pershing Square North\",id:\"519\",lat:\"40.751873\",long:\"-73.977706\"})" +
      "(v_Station_72:Station {name:\"Myrtle Ave & Lewis Ave\",id:\"3064\",lat:\"40.69681963\",long:\"-73.93756926\"})" +
      "(v_Station_73:Station {name:\"W Broadway & Spring Street\",id:\"3467\",lat:\"40.72494672359416\",long:\"-74.00165855884552\"})" +
      "(v_Station_74:Station {name:\"Schermerhorn St & Bond St\",id:\"3486\",lat:\"40.688417427540834\",long:\"-73.98451656103134\"})" +
      "(v_Station_75:Station {name:\"Jay St & York St\",id:\"3674\",lat:\"40.701403172577244\",long:\"-73.98672670125961\"})" +
      // Edges
      "(v_trip_33)-[e_start_station_0:start_station]->(v_Station_70)" +
      "(v_trip_41)-[e_start_station_1:start_station]->(v_Station_15)" +
      "(v_trip_32)-[e_start_station_2:start_station]->(v_Station_23)" +
      "(v_trip_1)-[e_used_bike_3:used_bike]->(v_Bike_54)" +
      "(v_trip_31)-[e_start_station_4:start_station]->(v_Station_5)" +
      "(v_trip_61)-[e_start_station_5:start_station]->(v_Station_37)" +
      "(v_trip_53)-[e_end_station_6:end_station]->(v_Station_50)" +
      "(v_trip_43)-[e_start_station_7:start_station]->(v_Station_71)" +
      "(v_trip_20)-[e_start_station_8:start_station]->(v_Station_24)" +
      "(v_trip_33)-[e_end_station_9:end_station]->(v_Station_75)" +
      "(v_trip_52)-[e_end_station_10:end_station]->(v_Station_59)" +
      "(v_trip_31)-[e_used_bike_11:used_bike]->(v_Bike_4)" +
      "(v_trip_26)-[e_start_station_12:start_station]->(v_Station_40)" +
      "(v_trip_25)-[e_start_station_13:start_station]->(v_Station_7)" +
      "(v_trip_53)-[e_used_bike_14:used_bike]->(v_Bike_65)" +
      "(v_trip_43)-[e_used_bike_15:used_bike]->(v_Bike_45)" +
      "(v_trip_10)-[e_end_station_16:end_station]->(v_Station_73)" +
      "(v_trip_0)-[e_end_station_17:end_station]->(v_Station_49)" +
      "(v_trip_20)-[e_end_station_18:end_station]->(v_Station_36)" +
      "(v_trip_2)-[e_end_station_19:end_station]->(v_Station_17)" +
      "(v_trip_52)-[e_start_station_20:start_station]->(v_Station_16)" +
      "(v_trip_18)-[e_start_station_21:start_station]->(v_Station_56)" +
      "(v_trip_26)-[e_used_bike_22:used_bike]->(v_Bike_28)" +
      "(v_trip_0)-[e_used_bike_23:used_bike]->(v_Bike_66)" +
      "(v_trip_43)-[e_end_station_24:end_station]->(v_Station_29)" +
      "(v_trip_32)-[e_used_bike_25:used_bike]->(v_Bike_62)" +
      "(v_trip_60)-[e_start_station_26:start_station]->(v_Station_14)" +
      "(v_trip_19)-[e_used_bike_27:used_bike]->(v_Bike_67)" +
      "(v_trip_61)-[e_used_bike_28:used_bike]->(v_Bike_22)" +
      "(v_trip_53)-[e_start_station_29:start_station]->(v_Station_47)" +
      "(v_trip_2)-[e_start_station_30:start_station]->(v_Station_39)" +
      "(v_trip_20)-[e_used_bike_31:used_bike]->(v_Bike_34)" +
      "(v_trip_52)-[e_used_bike_32:used_bike]->(v_Bike_63)" +
      "(v_trip_32)-[e_end_station_33:end_station]->(v_Station_73)" +
      "(v_trip_60)-[e_end_station_34:end_station]->(v_Station_35)" +
      "(v_trip_11)-[e_end_station_35:end_station]->(v_Station_55)" +
      "(v_trip_26)-[e_end_station_36:end_station]->(v_Station_40)" +
      "(v_trip_0)-[e_start_station_37:start_station]->(v_Station_72)" +
      "(v_trip_10)-[e_used_bike_38:used_bike]->(v_Bike_44)" +
      "(v_trip_41)-[e_used_bike_39:used_bike]->(v_Bike_27)" +
      "(v_trip_18)-[e_end_station_40:end_station]->(v_Station_8)" +
      "(v_trip_1)-[e_end_station_41:end_station]->(v_Station_9)" +
      "(v_trip_31)-[e_end_station_42:end_station]->(v_Station_6)" +
      "(v_trip_25)-[e_used_bike_43:used_bike]->(v_Bike_68)" +
      "(v_trip_42)-[e_end_station_44:end_station]->(v_Station_48)" +
      "(v_trip_33)-[e_used_bike_45:used_bike]->(v_Bike_3)" +
      "(v_trip_18)-[e_used_bike_46:used_bike]->(v_Bike_64)" +
      "(v_trip_60)-[e_used_bike_47:used_bike]->(v_Bike_21)" +
      "(v_trip_25)-[e_end_station_48:end_station]->(v_Station_74)" +
      "(v_trip_19)-[e_end_station_49:end_station]->(v_Station_51)" +
      "(v_trip_2)-[e_used_bike_50:used_bike]->(v_Bike_12)" +
      "(v_trip_41)-[e_end_station_51:end_station]->(v_Station_36)" +
      "(v_trip_11)-[e_used_bike_52:used_bike]->(v_Bike_13)" +
      "(v_trip_1)-[e_start_station_53:start_station]->(v_Station_58)" +
      "(v_trip_11)-[e_start_station_54:start_station]->(v_Station_46)" +
      "(v_trip_19)-[e_start_station_55:start_station]->(v_Station_30)" +
      "(v_trip_61)-[e_end_station_56:end_station]->(v_Station_38)" +
      "(v_trip_42)-[e_used_bike_57:used_bike]->(v_Bike_69)" +
      "(v_trip_42)-[e_start_station_58:start_station]->(v_Station_57)" +
      "(v_trip_10)-[e_start_station_59:start_station]->(v_Station_15)" + "]";

    // load data
    loader.initDatabaseFromString(gdlGraph);

    // get LogicalGraph representation of the social network graph
    LogicalGraph networkGraph = loader.getLogicalGraph();

    // transform to temporal graph by extracting time intervals from vertices
    return TemporalGraph.fromGraph(networkGraph)
      .transformVertices(TemporalCitiBikeGraph::extractTripPeriod);
  }

  /**
   * Function to extract the trip period from properties of a vertex. The names of the properties have to be
   * {@code starttime} and {@code stoptime} and their type has to be a String.
   *
   * @param current The current vertex to transform.
   * @param transformed A copy of the current vertex used to create the resulting one.
   * @return The resulting temporal vertex with assigned valid times.
   */
  private static TemporalVertex extractTripPeriod(TemporalVertex current, TemporalVertex transformed) {
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    try {
      String startTime = "starttime";
      if (current.hasProperty(startTime)) {
        transformed.setValidFrom(format.parse(current.getPropertyValue(startTime).getString()).getTime());
        transformed.removeProperty(startTime);
        String stopTime = "stoptime";
        if (current.hasProperty(stopTime)) {
          transformed.setValidTo(format.parse(current.getPropertyValue(stopTime).getString()).getTime());
          transformed.removeProperty(stopTime);
        }
      }
    } catch (ParseException e) {
      throw new RuntimeException("Can not parse time.");
    }
    return transformed;
  }
}
