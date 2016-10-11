package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Key;
import org.gradoop.flink.io.impl.csv.pojos.Label;
import org.gradoop.flink.io.impl.csv.pojos.Property;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

import java.util.regex.Pattern;

/**
 * Created by galpha on 9/28/16.
 */
public class CSVToEdge
  implements MapFunction<Tuple2<Csv, String>, ImportEdge<String>> {

  private final String datasourceName;

  private final String domainName;

  private ImportEdge<String> reuse;


  public CSVToEdge(String datasourceName, String domainName){
    this.datasourceName = datasourceName;
    this.domainName = domainName;
    this.reuse = new ImportEdge<>();
  }



  @Override
  public ImportEdge<String> map(Tuple2<Csv, String> tuple) throws Exception {


    Csv csv = tuple.f0;
    String line = tuple.f1;

    String[] fields = line.split(Pattern.quote(csv.getSeparator()));



    String key = createKey(csv.getEdge().getKey(), fields);
    String label = createLabel(csv.getEdge().getLabel(), fields);
    PropertyList properties = createProperties(csv, fields);


    reuse.setId(key);
    reuse.setLabel(label);
    reuse.setProperties(properties);



    return reuse;

  }

  private String createLabel(Label label, String[] fields) {

    String labelSeparator = label.getSeparator();
    String labelString = "";

    if (label.getRef() != null){

    }

    if (label.getReference() != null){

    }

    if (label.getRefOrReference() != null){

    }

    if (label.getStatic() != null){
      labelString = label.getStatic().getName();
    }

    return labelString;
  }

  private String createKey (Key key, String[] fields){

    String id = datasourceName + "_" + domainName + "_" + key.getClazz() + "_"
      + fields[0];

    return id;
  }

  private PropertyList createProperties (Csv csv, String[] fields){
    PropertyList list = PropertyList.create();



    for (Property p: csv.getEdge().getProperties().get(0).getProperty()) {
      org.gradoop.common.model.impl.properties.Property prop =
        new org.gradoop.common.model.impl.properties.Property();

      prop.setKey(p.getName());

      PropertyValue value = new PropertyValue();

      String type = csv.getColumns().getColumn().get(p.getColumnId()).getType
        ().value();

      if (type.equals("String")){
        value.setString(fields[p.getColumnId()]);
      } else if (type.equals("Integer")) {
        value.setInt(Integer.parseInt(fields[p.getColumnId()]));
      }


      prop.setValue(value);
      list.set(prop);
    }

    return list;
  }
}
