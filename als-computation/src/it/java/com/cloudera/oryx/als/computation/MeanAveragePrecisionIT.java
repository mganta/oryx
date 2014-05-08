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

package com.cloudera.oryx.als.computation;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.cloudera.oryx.als.computation.local.ALSLocalGenerationRunner;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobException;

/**
 * Tests use of {@code model.test-set-fraction}.
 * 
 * @author Sean Owen
 */
public final class MeanAveragePrecisionIT extends AbstractComputationIT {

  @Override
  protected Path getTestDataPath() {
    return getResourceAsFile("highlambda");
  }

  @Test
  public void testRunGenerationsWithTestSplit() throws Exception {
    // Call more times
    for (int gen = 1; gen <= 2; gen++) {
      copyDataAndRerun(gen);
    }
  }

  private void copyDataAndRerun(int gen) throws InterruptedException, JobException, IOException {
    Path generationInboundDir =
        Paths.get(ConfigUtils.getDefaultConfig().getString("model.instance-dir"), "0000" + gen, "inbound");
    for (Path file : IOUtils.listFiles(getTestDataPath())) {
      Files.copy(file, generationInboundDir.resolve(file.getFileName()));
    }
    new ALSLocalGenerationRunner().call();
  }

}
