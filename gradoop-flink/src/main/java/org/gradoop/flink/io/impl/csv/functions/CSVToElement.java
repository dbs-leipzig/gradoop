package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Key;
import org.gradoop.flink.io.impl.csv.pojos.Label;
import org.gradoop.flink.io.impl.csv.pojos.Properties;
import org.gradoop.flink.io.impl.csv.pojos.Property;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.List;
import java.util.regex.Pattern;


public class CSVToElement
  implements FlatMapFunction<Tuple2<Csv, List<String>>, EPGMElement> {

  private EPGMElement reuse;

  private Class type;

  private GraphHeadFactory graphHeadFactory;
  private VertexFactory vertexFactory;
  private EdgeFactory edgeFactory;


  public CSVToElement(Class type, GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory) {
    this.type = type;
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void flatMap(Tuple2<Csv, List<String>> tuple,
    Collector<EPGMElement> collector) throws Exception {

    Csv csv = tuple.f0;
    List<String> content = tuple.f1;

    boolean edge = false;



    for (String line : content) {

      String[] fields = line.split(Pattern.quote(csv.getSeparator()));

      String label = "";
      Key key = null;
      List<Properties> propertiesCsv = null;

      if(csv.getGraphhead() != null
        && type.isInstance(csv.getGraphhead())) {
        reuse = graphHeadFactory.createGraphHead();
        label = createLabel(csv.getGraphhead().getLabel(), fields);
        key = csv.getGraphhead().getKey();
        propertiesCsv = csv.getGraphhead().getProperties();

      } else if(csv.getVertex() != null
        && type.isInstance(csv.getVertex())) {
        reuse = vertexFactory.createVertex();
        label = createLabel(csv.getVertex().getLabel(), fields);
        key = csv.getVertex().getKey();
        propertiesCsv = csv.getVertex().getProperties();

      } else if(csv.getEdge() != null
        && type.isInstance(csv.getEdge())) {
        reuse = edgeFactory.createEdge(GradoopId.get(), GradoopId.get());
        edge = true;
        label = createLabel(csv.getEdge().getLabel(), fields);
        key = csv.getEdge().getKey();
        propertiesCsv = csv.getEdge().getProperties();

      } else {
        reuse = null;
      }

      PropertyList properties = createProperties(csv, propertiesCsv, key,
        fields);

      reuse.setId(GradoopId.get());
      reuse.setLabel(label);
      reuse.setProperties(properties);
      if (edge) {
        //TODO every static or ref column id has to be taken and then get the
        // source and target ids, also consider vertexEdges
        String sourceKey = createKey(csv.getEdge().getKey(),
          csv.getDatasourceName(), csv.getDomainName(), csv.getEdge()
            .getSource().toString());
        String targetKey = createKey(csv.getEdge().getKey(),
          csv.getDatasourceName(), csv.getDomainName(), csv.getEdge()
            .getTarget().toString());

        reuse.getProperties().set("source", sourceKey);
        reuse.getProperties().set("target", targetKey);

        collector.collect(reuse);
      }

    }
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

  private String createKey (Key key, String datasourceName, String domainName,
    String id) {

    String newId = escapeLowBar(datasourceName) + "_" + escapeLowBar(domainName)
      + "_" + escapeLowBar(key.getClazz()) + "_" + escapeLowBar(id);

    return newId;
  }

  private String escapeLowBar(String text) {
    StringBuilder result = new StringBuilder();
    StringCharacterIterator iterator = new StringCharacterIterator(text);
    char character = iterator.next();
    while (character != CharacterIterator.DONE) {
      if (character == '_') {
        result.append("&lowbar;");
      } else {
        result.append(character);
      }
      character = iterator.next();
    }
    return result.toString();
  }

  private PropertyList createProperties (Csv csv, List<Properties> properties,
    Key key, String[] fields) {
    PropertyList list = PropertyList.create();

    String resultKey = createKey(key,
      csv.getDatasourceName(), csv.getDomainName(), fields[0]);

    PropertyValue value = new PropertyValue();
    value.setString(resultKey);

    list.set("key", value);


    for (Property p: properties.get(0).getProperty()) {
      org.gradoop.common.model.impl.properties.Property prop =
        new org.gradoop.common.model.impl.properties.Property();

      prop.setKey(p.getName());

      value = new PropertyValue();

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
