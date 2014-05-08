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

package com.cloudera.oryx.kmeans.common.pmml;

import com.cloudera.oryx.common.math.Vectors;
import com.cloudera.oryx.kmeans.common.Centers;
import com.google.common.base.Splitter;
import org.apache.commons.math3.linear.RealVector;
import org.dmg.pmml.Array;
import org.dmg.pmml.Cluster;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.oryx.common.io.IOUtils;

/**
 * Convenience methods to read/write a {@link ClusteringModel} description of a k-means clustering.
 */
public final class KMeansPMML {

  // PMML always delimits with space
  private static final Splitter SPACE = Splitter.on(' ');

  private KMeansPMML() {
  }

  public static PMML read(Path path) throws IOException {
    try (InputStream in = IOUtils.openMaybeDecompressing(path)) {
      return read(in);
    } catch (JAXBException | SAXException jaxbe) {
      throw new IOException(jaxbe);
    }
  }

  public static PMML read(InputStream in) throws JAXBException, SAXException {
    return JAXBUtil.unmarshalPMML(ImportFilter.apply(new InputSource(in)));
  }

  public static void write(Path path, DataDictionary dictionary, List<? extends Model> models) throws IOException {
    try (OutputStream out = IOUtils.buildGZIPOutputStream(Files.newOutputStream(path))) {
      write(out, dictionary, models);
    } catch (JAXBException jaxbe) {
      throw new IOException(jaxbe);
    }
  }

  public static Centers toCenters(ClusteringModel cm) {
    int dims = cm.getClusteringFields().size();
    boolean sparse = cm.getMiningSchema().getMiningFields().size() * 2 < dims;
    List<RealVector> vecs = new ArrayList<>(cm.getClusters().size());
    for (Cluster c : cm.getClusters()) {
      vecs.add(createCenter(c.getArray(), sparse, dims));
    }
    return new Centers(vecs);
  }

  private static RealVector createCenter(Array array, boolean sparse, int fieldCount) {
    if (array.getN() != fieldCount) {
      return null;
    }
    RealVector v = sparse ? Vectors.sparse(fieldCount) : Vectors.dense(fieldCount);
    int i = 0;
    for (String token : SPACE.split(array.getValue())) {
      double t = Double.parseDouble(token);
      if (t != 0.0) {
        v.setEntry(i, t);
      }
      i++;
    }
    return v;
  }


  public static void write(
      OutputStream out,
      DataDictionary dictionary,
      List<? extends Model> models) throws JAXBException {
    PMML pmml = new PMML(null, dictionary, "4.2");
    pmml.getModels().addAll(models);
    JAXBUtil.marshalPMML(pmml, new StreamResult(out));
  }

}
