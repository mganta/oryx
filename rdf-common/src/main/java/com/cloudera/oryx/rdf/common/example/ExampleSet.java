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

package com.cloudera.oryx.rdf.common.example;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.util.FastMath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.rule.Decision;

/**
 * Encapsulates a set of {@link Example}s, including derived information about the data set, such
 * as the type of each feature and the type of the target.
 *
 * @author Sean Owen
 */
public final class ExampleSet implements Iterable<Example> {

  private final List<Example> examples;
  private final FeatureType[] featureTypes;
  private final int[] categoryCounts;
  private final FeatureType targetType;
  private final int targetCategoryCount;

  public ExampleSet(List<Example> examples) {
    Preconditions.checkNotNull(examples);
    Preconditions.checkArgument(!examples.isEmpty());
    this.examples = examples;

    InboundSettings inbound = InboundSettings.create(ConfigUtils.getDefaultConfig());
    int numFeatures = inbound.getColumnNames().size();

    featureTypes = new FeatureType[numFeatures];
    for (int i = 0; i < numFeatures; i++) {
      FeatureType type;
      if (inbound.isNumeric(i)) {
        type = FeatureType.NUMERIC;
      } else if (inbound.isCategorical(i)) {
        type = FeatureType.CATEGORICAL;
      } else {
        type = FeatureType.IGNORED;
      }
      featureTypes[i] = type;
    }

    int targetColumn = inbound.getTargetColumn();
    featureTypes[targetColumn] = FeatureType.IGNORED;
    targetType = inbound.isNumeric(targetColumn) ? FeatureType.NUMERIC : FeatureType.CATEGORICAL;

    categoryCounts = new int[numFeatures];

    int theTargetCategoryCount = 0;
    for (Example example : examples) {
      for (int i = 0; i < numFeatures; i++) {
        if (featureTypes[i] == FeatureType.CATEGORICAL) {
          CategoricalFeature feature = (CategoricalFeature) example.getFeature(i);
          if (feature != null) {
            categoryCounts[i] = FastMath.max(categoryCounts[i], feature.getValueID() + 1);
          }
        }
      }
      if (targetType == FeatureType.CATEGORICAL) {
        theTargetCategoryCount = FastMath.max(theTargetCategoryCount,
                                              ((CategoricalFeature) example.getTarget()).getValueID() + 1);
      }
    }
    this.targetCategoryCount = theTargetCategoryCount;
  }

  /**
   * For testing.
   */
  public ExampleSet(List<Example> examples, FeatureType[] featureTypes, FeatureType targetType) {
    Preconditions.checkNotNull(examples);
    Preconditions.checkArgument(!examples.isEmpty());
    this.examples = examples;

    this.featureTypes = featureTypes;
    this.targetType = targetType;
    int numFeatures = featureTypes.length;

    categoryCounts = new int[numFeatures];

    int theTargetCategoryCount = 0;
    for (Example example : examples) {
      for (int i = 0; i < numFeatures; i++) {
        if (featureTypes[i] == FeatureType.CATEGORICAL) {
          CategoricalFeature feature = (CategoricalFeature) example.getFeature(i);
          if (feature != null) {
            categoryCounts[i] = FastMath.max(categoryCounts[i], feature.getValueID() + 1);
          }
        }
      }
      if (targetType == FeatureType.CATEGORICAL) {
        theTargetCategoryCount = FastMath.max(theTargetCategoryCount,
                                              ((CategoricalFeature) example.getTarget()).getValueID() + 1);
      }
    }
    this.targetCategoryCount = theTargetCategoryCount;
  }

  private ExampleSet(List<Example> subset, ExampleSet of) {
    this.examples = subset;
    this.featureTypes = of.featureTypes;
    this.categoryCounts = of.categoryCounts;
    this.targetType = of.targetType;
    this.targetCategoryCount = of.targetCategoryCount;
  }

  public List<Example> getExamples() {
    return examples;
  }

  public int getNumFeatures() {
    return featureTypes.length;
  }

  public FeatureType getFeatureType(int featureNumber) {
    return featureTypes[featureNumber];
  }

  public int getCategoryCount(int featureNumber) {
    return categoryCounts[featureNumber];
  }

  public FeatureType getTargetType() {
    return targetType;
  }

  public int getTargetCategoryCount() {
    return targetCategoryCount;
  }

  @Override
  public Iterator<Example> iterator() {
    return examples.iterator();
  }

  public ExampleSet subset(List<Example> explicitSubset) {
    return new ExampleSet(explicitSubset, this);
  }

  public ExampleSet[] split(Decision decision) {
    List<Example> positive = new ArrayList<>();
    List<Example> negative = new ArrayList<>();
    for (Example example : examples) {
      if (decision.isPositive(example)) {
        positive.add(example);
      } else {
        negative.add(example);
      }
    }
    return new ExampleSet[] { subset(negative), subset(positive) };
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(Arrays.toString(featureTypes)).append(" -> ").append(targetType).append('\n');
    for (Example example : examples) {
      result.append(example).append('\n');
    }
    return result.toString();
  }

}
