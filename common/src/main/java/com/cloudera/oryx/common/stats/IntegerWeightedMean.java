/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.common.stats;

import java.io.Serializable;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.math3.stat.descriptive.AbstractStorelessUnivariateStatistic;

import com.cloudera.oryx.common.LangUtils;

/**
 * <p>Like {@link DoubleWeightedMean} but uses integer ({@code int}, {@code long}) weights.</p>
 *
 * <p>This class is not thread-safe.</p>
 *
 * @author Sean Owen
 * @see org.apache.commons.math3.stat.descriptive.moment.Mean
 */
public final class IntegerWeightedMean extends AbstractStorelessUnivariateStatistic implements Serializable {

  private long count;
  private long totalWeight;
  private double mean;
  
  public IntegerWeightedMean() {
    this(0, 0, Double.NaN);
  }
  
  private IntegerWeightedMean(long count, long totalWeight, double mean) {
    this.count = count;
    this.totalWeight = totalWeight;
    this.mean = mean;
  }

  @Override
  public IntegerWeightedMean copy() {
    return new IntegerWeightedMean(count, totalWeight, mean);
  }

  @Override
  public void clear() {
    count = 0;
    totalWeight = 0;
    mean = Double.NaN;
  }

  @Override
  public double getResult() {
    return mean;
  }

  @Override
  public long getN() {
    return count;
  }

  /**
   * @param datum new datum to add to the mean, with weight 1
   */
  @Override
  public void increment(double datum) {
    increment(datum, 1);
  }

  /**
   * @param datum new datum to add to the mean
   * @param weight weight of the new datum
   */
  public void increment(double datum, long weight) {
    Preconditions.checkArgument(weight >= 0);
    if (count == 0) {
      count = 1;
      mean = datum;
      totalWeight = weight;
    } else {
      count++;
      totalWeight += weight;
      mean += ((double) weight / totalWeight) * (datum - mean);
    }
  }

  /**
   * @param datum datum to remove from the mean
   * @param weight weight of that datum
   */
  public void decrement(double datum, long weight) {
    Preconditions.checkArgument(weight >= 0);
    Preconditions.checkState(count > 0);
    totalWeight -= weight;
    count--;
    mean -= ((double) weight / totalWeight) * (datum - mean);
  }

  @Override
  public String toString() {
    return Double.toString(mean);
  }

  @Override
  public int hashCode() {
    return Longs.hashCode(count) ^ Longs.hashCode(totalWeight) ^ LangUtils.hashDouble(mean);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IntegerWeightedMean)) {
      return false;
    }
    IntegerWeightedMean other = (IntegerWeightedMean) o;
    return count == other.count &&
        totalWeight == other.totalWeight &&
        Double.compare(mean, other.mean) == 0;
  }

}
