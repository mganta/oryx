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

package com.cloudera.oryx.als.computation.local;

import com.cloudera.oryx.als.computation.ALSJobStepConfig;
import com.cloudera.oryx.als.computation.modelbuilder.ALSModelBuilder;
import com.cloudera.oryx.computation.common.supplier.MRPipelineSupplier;
import com.google.common.io.Files;
import com.typesafe.config.Config;

import java.io.File;
import java.io.IOException;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.factorizer.MatrixFactorizer;
import com.cloudera.oryx.common.collection.LongFloatMap;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobException;
import com.cloudera.oryx.computation.common.LocalGenerationRunner;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.hadoop.conf.Configuration;

public final class ALSLocalGenerationRunner extends LocalGenerationRunner {

  @Override
  protected void runSteps() throws IOException {

    String instanceDir = getInstanceDir();
    int generationID = getGenerationID();
    String generationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID);
    int lastGenerationID = getLastGenerationID();

    File currentInboundDir = Files.createTempDir();
    currentInboundDir.deleteOnExit();
    File currentTrainDir = Files.createTempDir();
    currentTrainDir.deleteOnExit();
    File tempOutDir = Files.createTempDir();
    tempOutDir.deleteOnExit();
    File currentTestDir = new File(tempOutDir, "test");

    File lastInputDir = null;
    File lastMappingDir = null;
    File lastTestDir = null;
    if (lastGenerationID >= 0) {
      lastInputDir = Files.createTempDir();
      lastInputDir.deleteOnExit();
      lastMappingDir = Files.createTempDir();
      lastMappingDir.deleteOnExit();
      lastTestDir = Files.createTempDir();
      lastTestDir.deleteOnExit();
    }

    try {

      Store store = Store.get();
      store.downloadDirectory(generationPrefix + "inbound/", currentInboundDir);
      if (lastGenerationID >= 0) {
        String lastGenerationPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID);
        store.downloadDirectory(lastGenerationPrefix + "input/", lastInputDir);
        store.downloadDirectory(lastGenerationPrefix + "idMapping/", lastMappingDir);
        store.downloadDirectory(lastGenerationPrefix + "test/", lastTestDir);
      }

      ALSModelBuilder modelBuilder = new ALSModelBuilder(store);
      Pipeline sp = new SparkPipeline("local", "als", this.getClass());
      modelBuilder.build(sp.readTextFile(currentInboundDir.getAbsolutePath()),
          new ALSJobStepConfig(getInstanceDir(), getGenerationID(), getLastGenerationID(), 0, false));
      store.uploadDirectory(generationPrefix, tempOutDir, false);
      sp.done();

    } finally {
      IOUtils.deleteRecursively(currentInboundDir);
      IOUtils.deleteRecursively(currentTrainDir);
      IOUtils.deleteRecursively(currentTestDir);
      IOUtils.deleteRecursively(tempOutDir);
      IOUtils.deleteRecursively(lastInputDir);
      IOUtils.deleteRecursively(lastTestDir);
    }
  }

}
