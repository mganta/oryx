/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.rdf.serving.web;

import org.junit.Test;

import java.nio.file.Path;

import com.cloudera.oryx.kmeans.computation.local.KMeansLocalGenerationRunner;

public final class KMeansIssue34Conf2IT extends AbstractComputationIT {

  @Override
  protected Path getTestDataPath() {
    return getResourceAsFile("issue34Conf2");
  }

  @Test
  public void testKMeans() throws Exception {
    new KMeansLocalGenerationRunner().call();
  }

}
