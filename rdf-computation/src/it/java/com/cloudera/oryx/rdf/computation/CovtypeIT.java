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

package com.cloudera.oryx.rdf.computation;

import com.google.common.primitives.Doubles;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.computation.local.RDFLocalGenerationRunner;

/**
 * <p>Tests the random decision forest classifier using the
 * <a href="http://archive.ics.uci.edu/ml/datasets/Covertype">Covtype data set</a>, which contains
 * both numeric and categorical features and a categorical target.</p>
 *
 * <p>Covtype data set is copyright Jock A. Blackard and Colorado State University.</p>
 *
 * @author Sean Owen
 */
public final class CovtypeIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(CovtypeIT.class);

  @Override
  protected Path getTestDataPath() {
    return getResourceAsFile("covtype");
  }

  @Test
  public void testCovtype() throws Exception {
    List<Example> allExamples = readCovtypeExamples();
    DecisionForest forest = DecisionForest.fromExamplesWithDefault(allExamples);
    log.info("Evals: {}", forest.getEvaluations());
    assertTrue(new Mean().evaluate(forest.getEvaluations()) >= 0.8);
    double[] importances = forest.getFeatureImportances();
    log.info("Importances: {}", importances);
    assertNotNull(importances);
    for (double d : importances) {
      assertTrue(d >= 0.0);
      assertTrue(d <= 1.0);
    }
    assertEquals(importances[0], Doubles.max(importances));
    // Assert something about important features
    assertTrue(importances[0] > 0.9);
    assertTrue(importances[5] > 0.4);
    assertTrue(importances[9] > 0.4);
    assertTrue(importances[13] > 0.4);
  }

  @Test
  public void testPMMLOutput() throws Exception {
    new RDFLocalGenerationRunner().call();
    Path pmmlFile = TEST_TEMP_BASE_DIR.resolve("00000").resolve("model.pmml.gz");
    DecisionForestPMML.read(pmmlFile);
  }

  private static List<Example> readCovtypeExamples() throws IOException {
    List<Example> allExamples = new ArrayList<>();
    Pattern delimiter = Pattern.compile(",");
    Path dataFile = TEST_TEMP_INBOUND_DIR.resolve("covtype.csv.gz");
    for (CharSequence line : new FileLineIterable(dataFile)) {
      String[] tokens = delimiter.split(line);
      Feature[] features = new Feature[54];
      for (int i = 0; i < 10; i++) {
        features[i] = NumericFeature.forValue(Float.parseFloat(tokens[i]));
      }
      for (int i = 10; i < 54; i++) {
        features[i] = CategoricalFeature.forValue(Integer.parseInt(tokens[i]));
      }
      Example trainingExample = new Example(CategoricalFeature.forValue(Integer.parseInt(tokens[54]) - 1), features);
      allExamples.add(trainingExample);
    }
    return allExamples;
  }

}
