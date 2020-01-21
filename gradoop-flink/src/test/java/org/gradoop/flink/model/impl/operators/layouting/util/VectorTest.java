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
package org.gradoop.flink.model.impl.operators.layouting.util;

import org.junit.Assert;
import org.junit.Test;

public class VectorTest {

  @Test
  public void sub() {
    Vector a = new Vector(3, 3);
    Vector b = new Vector(1, 2);
    Vector result = a.sub(b);
    Assert.assertEquals(new Vector(2, 1), result);
  }

  @Test
  public void add() {
    Vector a = new Vector(3, 3);
    Vector b = new Vector(1, 2);
    Vector result = a.add(b);
    Assert.assertEquals(new Vector(4, 5), result);
  }

  @Test
  public void mul() {
    Vector a = new Vector(2, 3);
    Vector result = a.mul(5);
    Assert.assertEquals(new Vector(10, 15), result);
  }

  @Test
  public void div() {
    Vector a = new Vector(3, 6);
    Vector result = a.div(3);
    Assert.assertEquals(new Vector(1, 2), result);
  }

  @Test
  public void distance() {
    Vector a = new Vector(2, 1);
    Vector b = new Vector(1, 2);
    double dist = a.distance(b);
    Assert.assertEquals(Math.sqrt(2), dist, 0.0001f);
  }

  @Test
  public void clamped() {
    Vector a = new Vector(4, 2);
    Vector b = a.clamped(2);
    Vector c = a.clamped(100);
    Assert.assertEquals(c, a);
    Assert.assertEquals(2, b.magnitude(), 0.0001f);
    Assert.assertEquals(1, a.normalized().scalar(b.normalized()), 0.0001f);
    Assert.assertEquals(new Vector(0, 0), new Vector(0, 0).clamped(10));
  }

  @Test
  public void normalized() {
    Vector a = new Vector(4, 2);
    Vector b = a.normalized();
    Assert.assertEquals(1, b.magnitude(), 0.0001f);
    Assert.assertEquals(1, a.normalized().scalar(b.normalized()), 0.0001f);
    Assert.assertEquals(new Vector(0, 0), new Vector(0, 0).normalized());
  }

  @Test
  public void magnitude() {
    Vector b = new Vector(1, 1);
    double mag = b.magnitude();
    Assert.assertEquals(Math.sqrt(2), mag, 0.0001f);
  }

  @Test
  public void confined() {
    Vector a = new Vector(10, 50);
    Assert.assertEquals(a.confined(0, 100, 0, 100), a);
    Assert.assertEquals(a.confined(20, 100, 60, 100), new Vector(20, 60));
    Assert.assertEquals(a.confined(0, 5, 0, 10), new Vector(5, 10));
    Assert.assertEquals(a.confined(0, 100, 0, 10), new Vector(10, 10));
  }

  @Test
  public void testAngle() {
    Vector a = new Vector(0, 10);
    Vector b = new Vector(10, 0);
    Vector c = new Vector(0, -10);
    Vector d = new Vector(10, 10);

    Assert.assertEquals(0, a.angle(a), 0.000000001);
    Assert.assertEquals(90, a.angle(b), 0.0000001);
    Assert.assertEquals(180, a.angle(c), 0.00000001);
    Assert.assertEquals(45, a.angle(d), 0.00000001);
  }

  @Test
  public void testRotate() {
    Vector a = new Vector(100, 20);
    Vector b = a.rotate(87);
    Assert.assertEquals(a.magnitude(), b.magnitude(), 0.0000001);
    Assert.assertEquals(87, a.angle(b), 0.00000001);
  }

  @Test
  public void equals() {
    Vector a = new Vector(2, 1);
    Vector b = new Vector(1, 2);
    Assert.assertEquals(a, a);
    Assert.assertEquals(b, b);
    Assert.assertNotEquals(a, b);
    Assert.assertNotEquals(b, a);
    Assert.assertNotEquals(a, null);
    Assert.assertNotEquals(a, "test");
    Vector c = new Vector(2, 1);
    Assert.assertEquals(c, a);
  }

  @Test
  public void scalar() {
    Vector a = new Vector(2, 1);
    Vector b = new Vector(1, -2);
    Assert.assertEquals(a.scalar(b), 0, 0.0001f);
    Assert.assertEquals(a.normalized().scalar(a.normalized()), 1, 0.0001f);
    Assert.assertEquals(a.normalized().scalar(a.normalized().mul(-1)), -1, 0.0001f);

  }

  //-------------------------------------------------------------------

  @Test
  public void mSub() {
    Vector a = new Vector(3, 3);
    Vector b = new Vector(1, 2);
    a.mSub(b);
    Assert.assertEquals(new Vector(2, 1), a);
  }

  @Test
  public void mAdd() {
    Vector a = new Vector(3, 3);
    Vector b = new Vector(1, 2);
    a.mAdd(b);
    Assert.assertEquals(new Vector(4, 5), a);
  }

  @Test
  public void mMul() {
    Vector a = new Vector(2, 3);
    a.mMul(5);
    Assert.assertEquals(new Vector(10, 15), a);
  }

  @Test
  public void mDiv() {
    Vector a = new Vector(3, 6);
    a.mDiv(3);
    Assert.assertEquals(new Vector(1, 2), a);
  }

  @Test
  public void mClamped() {
    Vector a = new Vector(4, 2);
    a.mClamped(2);
    Assert.assertEquals(2, a.magnitude(), 0.0001f);
    Assert.assertEquals(1, a.normalized().scalar(a.normalized()), 0.0001f);
    Assert.assertEquals(new Vector(0, 0), new Vector(0, 0).clamped(10));
  }

  @Test
  public void mNormalized() {
    Vector a = new Vector(4, 2);
    Vector b = a.mNormalized();
    Assert.assertEquals(1, b.magnitude(), 0.0001f);
    Assert.assertEquals(1, a.normalized().scalar(b.normalized()), 0.0001f);
    Assert.assertEquals(new Vector(0, 0), new Vector(0, 0).normalized());
  }


  @Test
  public void mConfined() {
    Vector a = new Vector(10, 50);
    Assert.assertEquals(a.mConfined(0, 100, 0, 100), a);
    a = new Vector(10, 50);
    Assert.assertEquals(a.mConfined(20, 100, 60, 100), new Vector(20, 60));
    a = new Vector(10, 50);
    Assert.assertEquals(a.mConfined(0, 5, 0, 10), new Vector(5, 10));
    a = new Vector(10, 50);
    Assert.assertEquals(a.mConfined(0, 100, 0, 10), new Vector(10, 10));
  }

  @Test
  public void testmRotate() {
    Vector a = new Vector(100, 20);
    Vector b = a.rotate(87);
    a.mRotate(87);
    Assert.assertEquals(a, b);
  }
}
