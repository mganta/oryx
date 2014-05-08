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

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.als.common.TopN;
import com.cloudera.oryx.als.computation.similar.MostSimilarItemIterator;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.LongPrimitiveIterator;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

final class MakeItemSimilarity implements Callable<Object> {

  private static final Logger log = LoggerFactory.getLogger(MakeItemSimilarity.class);

  private final Path modelDir;
  private final LongObjectMap<float[]> Y;
  private final StringLongMapping idMapping;

  MakeItemSimilarity(Path modelDir, LongObjectMap<float[]> Y, StringLongMapping idMapping) {
    this.modelDir = modelDir;
    this.Y = Y;
    this.idMapping = idMapping;
  }

  @Override
  public Object call() throws IOException {

    log.info("Starting item similarity");

    Config config = ConfigUtils.getDefaultConfig();
    final int howMany = config.getInt("model.item-similarity.how-many");

    final LongPrimitiveIterator it = Y.keySetIterator();

    final Path similarItemsDir = modelDir.resolve("similarItems");
    Files.createDirectories(similarItemsDir);

    ExecutorService executor = ExecutorUtils.buildExecutor("ItemSimilarity");
    Collection<Future<Object>> futures = new ArrayList<>();

    try {
      int numThreads = ExecutorUtils.getParallelism();
      for (int i = 0; i < numThreads; i++) {
        final int workerNumber = i;
        futures.add(executor.submit(new Callable<Object>() {
          @Override
          public Void call() throws IOException {
            try (Writer out = IOUtils.buildGZIPWriter(similarItemsDir.resolve(workerNumber + ".csv.gz"))) {
              while (true) {
                long itemID;
                synchronized (it) {
                  if (!it.hasNext()) {
                    return null;
                  }
                  itemID = it.nextLong();
                }
                float[] itemFeatures = Y.get(itemID);
                Iterable<NumericIDValue> mostSimilar = TopN.selectTopN(
                    new MostSimilarItemIterator(Y.entrySet().iterator(), itemID, itemFeatures), howMany);
                String item1IDString = idMapping.toString(itemID);
                for (NumericIDValue similar : mostSimilar) {
                  out.write(DelimitedDataUtils.encode(',',
                      item1IDString,
                      idMapping.toString(similar.getID()),
                      Float.toString(similar.getValue())));
                  out.write('\n');
                }
              }
            }
          }
        }));

      }
      ExecutorUtils.getResults(futures);
    } finally {
      ExecutorUtils.shutdownNowAndAwait(executor);
    }

    log.info("Finished item similarity");

    return null;
  }

}
