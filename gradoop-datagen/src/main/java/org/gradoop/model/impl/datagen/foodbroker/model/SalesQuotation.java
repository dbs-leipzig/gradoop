package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyList;

/**
 * f0 : case id
 * f1/f2 : sentBy
 * f3/f3 : sentTo
 */
public class SalesQuotation
  extends Tuple7<Long, Long, Short, Long, Short, GradoopId, PropertyList> {

  public static final String STATUS_KEY = "status";
  public static final String STATUS_OPEN = "open";
  public static final String STATUS_LOST = "lost";
  public static final String STATUS_WON = "confirmed";

  public SalesQuotation() {

  }

  public SalesQuotation(Long caseId) {
    setId(GradoopId.get());
    setCaseId(caseId);
    setSentByQuality((short) 0);
    setSentToQuality((short) 0);

    PropertyList properties = new PropertyList();

    properties.set("bid", "SQN" + caseId.toString());
    properties.set(STATUS_KEY, STATUS_OPEN);

    setProperties(properties);
  }

  public void setSentBy(Long sentBy) {
    this.f1 = sentBy;
  }

  public void setSentByQuality(Short sentByQuality) {
    this.f2 = sentByQuality;
  }

  public void setSentTo(Long sentTo) {
    this.f3 = sentTo;
  }

  public void setSentToQuality(Short sentToQuality) {
    this.f4 = sentToQuality;
  }

  public Long getCaseId() {
    return this.f0;
  }

  public void setCaseId(Long caseID) {
    this.f0 = caseID;
  }

  public void setId(GradoopId id) {
    this.f5 = id;
  }

  public GradoopId getId() {
    return this.f5;
  }

  public void setProperties(PropertyList properties) {
    this.f6 = properties;
  }

  public Short getSentByQuality() {
    return this.f2;
  }

  public PropertyList getProperties() {
    return this.f6;
  }

  public static TypeInformation<SalesQuotation> getType() {
    return new TupleTypeInfo<>(
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.SHORT_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.SHORT_TYPE_INFO,
      TypeExtractor.createTypeInfo(GradoopId.class),
      TypeExtractor.createTypeInfo(PropertyList.class)
    );
  }
}
