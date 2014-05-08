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

package com.cloudera.oryx.kmeans.computation.local;

import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kmeans.common.ClusterValidityStatistics;
import com.cloudera.oryx.kmeans.common.KMeansEvalStrategy;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.evaluate.EvaluationSettings;
import com.cloudera.oryx.kmeans.computation.evaluate.KMeansEvaluationData;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public final class ClusteringEvaluation implements Callable<List<KMeansEvaluationData>> {

  private final List<List<WeightedRealVector>> weightedPoints;

  public ClusteringEvaluation(List<List<WeightedRealVector>> weightedPoints) {
    this.weightedPoints = weightedPoints;
  }

  @Override
  public List<KMeansEvaluationData> call() {
    Config config = ConfigUtils.getDefaultConfig();
    EvaluationSettings settings = EvaluationSettings.create(config);

    List<KMeansEvaluationData> evalData;
    ExecutorService exec = ExecutorUtils.buildExecutor("KMEANS");
    try {
      List<Future<KMeansEvaluationData>> futures = new ArrayList<>();
      for (Integer nc : settings.getKValues()) {
        int loops = nc == 1 ? 1 : settings.getReplications();
        for (int i = 0; i < loops; i++) {
          futures.add(exec.submit(new EvaluationRun(weightedPoints, nc, i, settings)));
        }
      }
      evalData = ExecutorUtils.getResults(futures);
    } finally {
      ExecutorUtils.shutdownNowAndAwait(exec);
    }

    KMeansEvalStrategy evalStrategy = settings.getEvalStrategy();
    if (evalStrategy != null) {
      List<ClusterValidityStatistics> best = evalStrategy.evaluate(Lists.transform(evalData,
          new Function<KMeansEvaluationData, ClusterValidityStatistics>() {
            @Override
            public ClusterValidityStatistics apply(KMeansEvaluationData input) {
              return input.getClusterValidityStatistics();
            }
          }));
      if (best.size() == 1) {
        ClusterValidityStatistics cvs = best.get(0);
        for (KMeansEvaluationData ed : evalData) {
          if (cvs.getK() == ed.getK() && cvs.getReplica() == ed.getReplica()) {
            return ImmutableList.of(ed);
          }
        }
      }
    }
    return evalData;
  }

}
