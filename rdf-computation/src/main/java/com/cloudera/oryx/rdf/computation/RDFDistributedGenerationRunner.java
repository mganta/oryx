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

import com.google.common.base.Preconditions;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Segment;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.DependsOn;
import com.cloudera.oryx.computation.common.DistributedGenerationRunner;
import com.cloudera.oryx.computation.common.JobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.rdf.computation.build.BuildTreesStep;
import com.cloudera.oryx.rdf.computation.build.MergeNewOldStep;

/**
 * @author Sean Owen
 */
public final class RDFDistributedGenerationRunner extends DistributedGenerationRunner {

  private static final Logger log = LoggerFactory.getLogger(RDFDistributedGenerationRunner.class);

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPreDependencies() {
    return Collections.emptyList();
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getIterationDependencies() {
    return Collections.singletonList(
        DependsOn.<Class<? extends JobStep>>nextAfterFirst(BuildTreesStep.class, MergeNewOldStep.class));
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPostDependencies() {
    return Collections.emptyList();
  }

  @Override
  protected JobStepConfig buildConfig(int iteration) {
    return new RDFJobStepConfig(getInstanceDir(),
                                getGenerationID(),
                                getLastGenerationID(),
                                iteration);
  }

  @Override
  protected void doPost() throws IOException {

    String instanceGenerationPrefix =
        Namespaces.getInstanceGenerationPrefix(getInstanceDir(), getGenerationID());
    String outputPathKey = instanceGenerationPrefix + "trees/";
    Store store = Store.get();
    PMML joinedForest = null;

    // TODO This is still loading all trees into memory, which can be quite large.
    // To do better we would have to manage XML output more directly.

    Map<String,Mean> columnNameToMeanImportance = new HashMap<>();

    for (String treePrefix : store.list(outputPathKey, true)) {
      log.info("Reading trees from file {}", treePrefix);
      for (String treePMMLAsLine : new FileLineIterable(store.readFrom(treePrefix))) {
        PMML treePMML;
        try {
          treePMML = JAXBUtil.unmarshalPMML(
              ImportFilter.apply(new InputSource(new StringReader(treePMMLAsLine))));
        } catch (JAXBException | SAXException e) {
          throw new IOException(e);
        }

        if (joinedForest == null) {
          joinedForest = treePMML;
          updateMeanImportances(columnNameToMeanImportance, treePMML.getModels().get(0));
        } else {
          MiningModel existingModel = (MiningModel) joinedForest.getModels().get(0);
          MiningModel nextModel = (MiningModel) treePMML.getModels().get(0);
          updateMeanImportances(columnNameToMeanImportance, nextModel);
          existingModel.getSegmentation().getSegments().addAll(nextModel.getSegmentation().getSegments());
        }
      }
    }

    Preconditions.checkNotNull(joinedForest, "No forests to join?");

    MiningModel model = (MiningModel) joinedForest.getModels().get(0);

    // Renumber segments with distinct IDs
    List<Segment> segments = model.getSegmentation().getSegments();
    for (int treeID = 0; treeID < segments.size(); treeID++) {
      segments.get(treeID).setId(Integer.toString(treeID));
    }

    // Stitch together feature importances
    for (MiningField field : model.getMiningSchema().getMiningFields()) {
      String name = field.getName().getValue();
      Mean importance = columnNameToMeanImportance.get(name);
      if (importance == null) {
        field.setImportance(null);
      } else {
        field.setImportance(importance.getResult());
      }
    }

    log.info("Writing combined model file");
    Path tempJoinedForestFile = IOUtils.createTempFile("model-", ".pmml.gz");
    try (OutputStream out = IOUtils.buildGZIPOutputStream(Files.newOutputStream(tempJoinedForestFile))) {
      JAXBUtil.marshalPMML(joinedForest, new StreamResult(out));
    } catch (JAXBException e) {
      throw new IOException(e);
    }

    log.info("Uploading combined model file");
    store.upload(instanceGenerationPrefix + "model.pmml.gz", tempJoinedForestFile, false);
    Files.delete(tempJoinedForestFile);
  }

  private static void updateMeanImportances(Map<String,Mean> columnNameToMeanImportance, Model model) {
    for (MiningField field : model.getMiningSchema().getMiningFields()) {
      Double importance = field.getImportance();
      if (importance != null) {
        String fieldName = field.getName().getValue();
        Mean mean = columnNameToMeanImportance.get(fieldName);
        if (mean == null) {
          mean = new Mean();
          columnNameToMeanImportance.put(fieldName, mean);
        }
        mean.increment(importance);
      }
    }
  }

}
