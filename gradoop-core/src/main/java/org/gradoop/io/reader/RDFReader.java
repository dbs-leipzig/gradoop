package org.gradoop.io.reader;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.EdgeFactory;
import org.gradoop.model.impl.VertexFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads RDF input data serialized as NTriples.
 */
public class RDFReader implements VertexLineReader {
  /**
   * Separates a line into tokens, combinations of whitespace/tab possible.
   */
  private static final Pattern LINE_TOKEN_SEPARATOR =
    Pattern.compile("[ *|\\t*]+");

  /**
   * Remove < and > from single token string having < as first and > as last
   * char.
   */
  private static final Pattern RESOURCE = Pattern.compile("^\\<(.*)\\>$");

  /**
   * Get i.e. 'integer' from
   * "539348"^^<http://www.w3.org/2001/XMLSchema#integer>
   */
  private static final Pattern LITERAL_DATATYPE =
    Pattern.compile("\".*\"\\^\\^.*[#|:]([a-z|A-Z]+)");

  /**
   * Get i.e. '539348' from
   * "539348\"^^<http://www.w3.org/2001/XMLSchema#integer>
   */
  private static final Pattern LITERAL_VALUE = Pattern.compile("\"(.*)\"");

  /**
   * Define MD5 HASH_FUNCTION function.
   */
  private static final HashFunction HASH_FUNCTION = Hashing.md5();

  /**
   * Separates the whole line using {@code LINE_TOKEN_SEPARATOR}.
   *
   * @param line single input line
   * @return token array
   */
  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  /**
   * Checks token for starting '<' and ending '>'
   *
   * @param token part of a RDF triple
   * @return token without '<' and '>'
   */
  private boolean isNTResource(String token) {
    return RESOURCE.matcher(token).matches();
  }

  /**
   * Retrieves plain string from i.e., <www.example.net>
   *
   * @param token part of a RDF triple
   * @return token without '<' and '>'
   */
  private String getPlainResource(String token) {
    Matcher m = RESOURCE.matcher(token);
    if (m.matches()) {
      return m.group(1);
    }
    return "";
  }

  /**
   * Get data type of RDF literal, if given
   *
   * @param token literal (RDF object)
   * @return extracted data type
   */
  private String getDatatype(String token) {
    Matcher m = LITERAL_DATATYPE.matcher(token);
    if (m.find()) {
      return m.group(1);
    }
    return "";
  }

  /**
   * Get value of RDF literal
   *
   * @param token literal (RDF object)
   * @return literal value
   */
  private String getLiteralValue(String token) {
    Matcher m = LITERAL_VALUE.matcher(token);
    if (m.find()) {
      return m.group(1);
    }
    return "";
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(String line) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Vertex> readVertexList(String line) {
    String[] triple = simpleValidate(line);
    if (triple.length == 0) {
      return null;
    }
    String s = triple[0];
    String p = triple[1];
    String o = triple[2];

    List<Vertex> vList = Lists.newArrayListWithCapacity(2);
    Long sourceID =
      HASH_FUNCTION.newHasher().putString(s, Charsets.UTF_8).hash().asLong();
    Long targetID =
      HASH_FUNCTION.newHasher().putString(o, Charsets.UTF_8).hash().asLong();

    if (o.startsWith("\"")) { // create only one vertex with property
      Vertex vertex =
        VertexFactory.createDefaultVertexWithLabel(sourceID, s, null);

      switch (getDatatype(o)) {
      case "integer":
        vertex.setProperty(p, Integer.parseInt(getLiteralValue(o)));
        break;
      case "double":
      case "float":
      case "decimal":
        vertex.setProperty(p, Double.parseDouble(getLiteralValue(o)));
        break;
      default:
        vertex.setProperty(p, getLiteralValue(o));
        break;
      }
      vList.add(vertex);
    } else { // 2x resource -> 2x vertex
      //outgoing edge on source vertex
      Long edgeOutIndex =
        HASH_FUNCTION.newHasher().putString(s + p + o, Charsets.UTF_8).hash()
          .asLong();
      Edge edgeOut =
        EdgeFactory.createDefaultEdgeWithLabel(targetID, p, edgeOutIndex);
      Vertex sourceVertex = VertexFactory
        .createDefaultVertexWithLabel(sourceID, s, Lists.newArrayList(edgeOut));
      vList.add(sourceVertex);

      //incoming edge on target vertex
      Long edgeInIndex =
        HASH_FUNCTION.newHasher().putString(o + p + s, Charsets.UTF_8).hash()
          .asLong();
      Edge edgeIn =
        EdgeFactory.createDefaultEdgeWithLabel(sourceID, p, edgeInIndex);
      Vertex targetVertex = VertexFactory
        .createDefaultVertexWithEdges(targetID, null,
          Lists.newArrayList(edgeIn));
      targetVertex.setLabel(o);
      vList.add(targetVertex);
    }
    return vList;
  }

  /**
   * Simple validation of input line, not complete.
   *
   * @param line single input line
   * @return input line or null for malformed triple
   */
  private String[] simpleValidate(String line) {
    String[] empty = {};
    String[] tokens = getTokens(line);
    String s = tokens[0];
    String p = tokens[1];
    String o = tokens[2];

    if (isNTResource(p)) { // predicate has to be URI
      if (isNTResource(s) || s.charAt(0) == '_') {
        //subject has to be blank node or URI
        if (isNTResource(o) || o.charAt(0) == '_' || o.charAt(0) == '"') {
          s = getPlainResource(s);
          p = getPlainResource(p);
          if (isNTResource(o)) {
            o = getPlainResource(o);
          }

          return new String[]{s, p, o};
        } else {
          return empty; // malformed object
        }
      } else {
        return empty; // malformed subject
      }
    } else {
      return empty; // malformed predicate
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsVertexLists() {
    return true;
  }

}
