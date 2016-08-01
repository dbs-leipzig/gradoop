package org.gradoop.model.impl.datagen.foodbroker.tuples;

import org.gradoop.model.impl.id.GradoopId;

import java.math.BigDecimal;

/**
 * Created by Stephan on 28.07.16.
 */
public class ProductTuple extends AbstractMasterDataTuple {//implements
// MDTuple {

//  private GradoopId f0;
//  private Float f1;
  private BigDecimal f2;


  /**
   * default constructor
   */
  public ProductTuple() {
  }

  /**
   * valued constructor
   * @param id id gradoop id
   * @param quality master data quality
   */
  public ProductTuple(GradoopId id, Float quality, BigDecimal price) {
    super(id, quality);
    f2 = price;

  }

  public BigDecimal getPrice() {
    return f2;
  }

  @Override
  public String toString() {
    return "(" + f0 + ", " + f1 + ", " + f2 + ")";
  }
}
