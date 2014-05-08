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

package com.cloudera.oryx.als.computation.local;

import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;

final class SplitTestTrain implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(SplitTestTrain.class);

  private final Path inboundDir;
  private final Path trainDir;
  private final Path testDir;

  SplitTestTrain(Path inboundDir, Path trainDir, Path testDir) {
    this.inboundDir = inboundDir;
    this.trainDir = trainDir;
    this.testDir = testDir;
  }

  @Override
  public Void call() throws IOException {
    List<Path> inputFiles = IOUtils.listFiles(inboundDir);
    if (inputFiles.isEmpty()) {
      log.info("No input files in {}", inboundDir);
      return null;
    }
    Collections.sort(inputFiles, ByLastModifiedComparator.INSTANCE);

    Files.createDirectories(trainDir);
    Files.createDirectories(testDir);

    double testSetFraction = ConfigUtils.getDefaultConfig().getDouble("model.test-set-fraction");

    if (testSetFraction == 0.0) {
      for (Path inputFile : inputFiles) {
        log.info("Copying {} to {}", inputFile, trainDir);
        Files.copy(inputFile, trainDir.resolve(inputFile.getFileName()));
      }
    } else {
      RandomGenerator random = RandomManager.getRandom();
      try (Writer trainOut = IOUtils.buildGZIPWriter(trainDir.resolve("train.csv.gz"));
           Writer testOut = IOUtils.buildGZIPWriter(testDir.resolve("test.csv.gz"))) {
        for (Path inputFile : inputFiles) {
          log.info("Splitting {}", inputFile);
          for (CharSequence line : new FileLineIterable(inputFile)) {
            if (random.nextDouble() < testSetFraction) {
              testOut.append(line).append('\n');
            } else {
              trainOut.append(line).append('\n');
            }
          }
        }
      }
    }

    return null;
  }

}
