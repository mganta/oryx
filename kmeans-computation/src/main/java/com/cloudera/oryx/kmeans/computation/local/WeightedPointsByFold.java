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
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kmeans.common.WeightedRealVector;
import com.cloudera.oryx.kmeans.computation.cluster.ClusterSettings;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchIndex;
import com.typesafe.config.Config;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public final class WeightedPointsByFold implements Callable<List<List<WeightedRealVector>>> {

  private static final Logger log = LoggerFactory.getLogger(WeightedPointsByFold.class);

  private final List<List<RealVector>> foldVecs;

  public WeightedPointsByFold(List<List<RealVector>> foldVecs) {
    this.foldVecs = foldVecs;
  }

  @Override
  public List<List<WeightedRealVector>> call() {
    Config config = ConfigUtils.getDefaultConfig();
    ClusterSettings cluster = ClusterSettings.create(config);
    KSketchIndex index = buildIndex(foldVecs, cluster);
    int pointsPerIteration = cluster.getSketchPoints();
    RandomGenerator random = RandomManager.getRandom();

    ExecutorService exec = ExecutorUtils.buildExecutor("KSKETCH");
    try {
      for (int iter = 0; iter < cluster.getSketchIterations(); iter++) {
        log.info("Starting sketch iteration {}", iter + 1);
        List<Future<Collection<RealVector>>> futures = new ArrayList<>();
        for (int foldId = 0; foldId < foldVecs.size(); foldId++) {
          futures.add(exec.submit(
              new SamplingRun(index, random, foldId, foldVecs.get(foldId), pointsPerIteration)));
        }
        // At the end of each iteration, gather up the sampled points to add to the index
        List<Collection<RealVector>> newSamples = ExecutorUtils.getResults(futures);
        for (int foldId = 0; foldId < foldVecs.size(); foldId++) {
          for (RealVector v : newSamples.get(foldId)) {
            index.add(v, foldId);
          }
        }
        index.rebuildIndices();
      }

      List<Future<List<WeightedRealVector>>> ret = new ArrayList<>();
      for (int foldId = 0; foldId < foldVecs.size(); foldId++) {
        ret.add(exec.submit(new AssignmentRun(index, foldId, foldVecs.get(foldId))));
      }
      return ExecutorUtils.getResults(ret);
    } finally {
      ExecutorUtils.shutdownNowAndAwait(exec);
    }
  }

  private static KSketchIndex buildIndex(List<List<RealVector>> foldVecs, ClusterSettings settings) {
    if (foldVecs.isEmpty()) {
      throw new IllegalStateException("No input vectors found for sketch building");
    }
    KSketchIndex index = new KSketchIndex(foldVecs.size(), foldVecs.get(0).get(0).getDimension(),
        settings.getIndexBits(), settings.getIndexSamples(), 1729L);
    for (int i = 0; i < foldVecs.size(); i++) {
      if (!foldVecs.get(i).isEmpty()) {
        index.add(foldVecs.get(i).get(0), i);
      }
    }
    index.rebuildIndices();
    return index;
  }
}
