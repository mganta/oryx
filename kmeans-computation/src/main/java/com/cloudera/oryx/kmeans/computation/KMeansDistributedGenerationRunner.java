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

package com.cloudera.oryx.kmeans.computation;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.kmeans.computation.cluster.KSketchSamplingStep;
import com.cloudera.oryx.kmeans.computation.covariance.CovarianceStep;
import com.cloudera.oryx.kmeans.computation.evaluate.ClusteringStep;
import com.cloudera.oryx.kmeans.computation.evaluate.VoronoiPartitionStep;
import com.cloudera.oryx.kmeans.computation.normalize.NormalizeStep;
import com.cloudera.oryx.kmeans.computation.outlier.OutlierStep;
import com.cloudera.oryx.kmeans.computation.summary.SummaryStep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.oryx.computation.common.DependsOn;
import com.cloudera.oryx.computation.common.DistributedGenerationRunner;
import com.cloudera.oryx.computation.common.JobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KMeansDistributedGenerationRunner extends DistributedGenerationRunner {

  private static final Logger log = LoggerFactory.getLogger(KMeansDistributedGenerationRunner.class);

  @Override
  protected void doPre() throws IOException {
    // Verify that id-columns are provided in the config if outlier computations are enabled.
    Config config = ConfigUtils.getDefaultConfig();
    if (doOutlierComputation(config)) {
      InboundSettings inbound = InboundSettings.create(config);
      if (inbound.getIdColumns().isEmpty()) {
        String msg = "id-column(s) value must be specified if model.outliers.compute is enabled";
        log.error(msg);
        throw new IllegalStateException("Invalid k-means configuration: " + msg);
      }
    }
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPreDependencies() {
    List<DependsOn<Class<? extends JobStep>>> preDeps = new ArrayList<>();
    preDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(NormalizeStep.class, SummaryStep.class));
    return preDeps;
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getIterationDependencies() {
    List<DependsOn<Class<? extends JobStep>>> iterationsDeps = new ArrayList<>();
    iterationsDeps.add(DependsOn.<Class<? extends JobStep>>first(KSketchSamplingStep.class));
    return iterationsDeps;
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPostDependencies() {
    Config config = ConfigUtils.getDefaultConfig();
    List<DependsOn<Class<? extends JobStep>>> postDeps = new ArrayList<>();
    postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(ClusteringStep.class, VoronoiPartitionStep.class));
    if (doOutlierComputation(config)) {
      if (config.getBoolean("model.outliers.mahalanobis")) {
        postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(CovarianceStep.class, ClusteringStep.class));
        postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(OutlierStep.class, CovarianceStep.class));
      } else {
        postDeps.add(DependsOn.<Class<? extends JobStep>>nextAfterFirst(OutlierStep.class, ClusteringStep.class));
      }
    }
    return postDeps;
  }

  private static boolean doOutlierComputation(Config config) {
    return config.hasPath("model.outliers") && config.getBoolean("model.outliers.compute");
  }

  @Override
  protected JobStepConfig buildConfig(int iteration) {
    return new KMeansJobStepConfig(
        getInstanceDir(),
        getGenerationID(),
        getLastGenerationID(),
        iteration);
  }

  @Override
  protected boolean areIterationsDone(int iterationNumber) {
    return iterationNumber >= ConfigUtils.getDefaultConfig().getInt("model.sketch-iterations");
  }

}
