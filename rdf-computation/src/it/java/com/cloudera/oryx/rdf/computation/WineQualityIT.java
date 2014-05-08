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
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.computation.local.RDFLocalGenerationRunner;

/**
 * <p>Tests the random decision forest classifier on a
 * <a href="http://archive.ics.uci.edu/ml/datasets/Wine+Quality">wine quality</a> data set. This contains
 * a numeric features and a numeric target.</p>
 *
 * <p>Citation for data set: P. Cortez, A. Cerdeira, F. Almeida, T. Matos and J. Reis. Modeling wine 
 * preferences by data mining from physicochemical properties. In Decision Support Systems, Elsevier,
 * 47(4):547-553, 2009.</p>
 *
 * @author Sean Owen
 */
public final class WineQualityIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(WineQualityIT.class);

  @Override
  protected Path getTestDataPath() {
    return getResourceAsFile("winequality");
  }

  @Test
  public void testWineQuality() throws Exception {
    List<Example> allExamples = readWineQualityExamples();
    DecisionForest forest = DecisionForest.fromExamplesWithDefault(allExamples);
    log.info("Evals: {}", forest.getEvaluations());
    assertTrue(new Mean().evaluate(forest.getEvaluations()) < 1.2);
    double[] importances = forest.getFeatureImportances();
    log.info("Importances: {}", importances);
    for (double d : importances) {
      assertTrue(d >= 0.0);
      assertTrue(d <= 1.0);
    }
    assertEquals(importances[8], Doubles.max(importances));
    assertTrue(importances[1] > 0.6);
    assertTrue(importances[5] > 0.6);
    assertTrue(importances[8] > 0.8);
    assertTrue(importances[9] > 0.6);
    assertTrue(importances[10] > 0.7);
  }

  @Test
  public void testPMMLOutput() throws Exception {
    new RDFLocalGenerationRunner().call();
    Path pmmlFile = TEST_TEMP_BASE_DIR.resolve("00000").resolve("model.pmml.gz");
    DecisionForestPMML.read(pmmlFile);
  }

  private static List<Example> readWineQualityExamples() throws IOException {
    List<Example> allExamples = new ArrayList<>();
    Pattern delimiter = Pattern.compile(";");
    Path dataFile = TEST_TEMP_INBOUND_DIR.resolve("winequality-white.csv");
    for (CharSequence line : new FileLineIterable(dataFile)) {
      if (line.length() == 0) {
        continue;
      }
      String[] tokens = delimiter.split(line);
      Feature[] features = new Feature[11];
      for (int i = 0; i < features.length; i++) {
        features[i] = NumericFeature.forValue(Float.parseFloat(tokens[i]));
      }
      Example trainingExample = new Example(NumericFeature.forValue(Float.parseFloat(tokens[11])), features);
      allExamples.add(trainingExample);
    }
    return allExamples;
  }

}
