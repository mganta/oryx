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

package com.cloudera.oryx.als.serving.web;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.math3.util.Pair;

import com.cloudera.oryx.als.common.rescorer.Rescorer;
import com.cloudera.oryx.common.LangUtils;
import com.cloudera.oryx.als.common.NoSuchItemException;
import com.cloudera.oryx.als.common.NotReadyException;
import com.cloudera.oryx.als.common.OryxRecommender;
import com.cloudera.oryx.als.common.rescorer.RescorerProvider;

/**
 * <p>Responds to a GET request to
 * {@code /recommendToAnonymous/[itemID1(=value1)](/[itemID2(=value2)]/...)(?howMany=n)(&offset=o)(&rescorerParams=...)},
 * and in turn calls {@link OryxRecommender#recommendToAnonymous(String[], float[], int, Rescorer)}
 * with the supplied values. That is, 1 or more item IDs are supplied, which may each optionally correspond to
 * a value or else default to 1.
 * {@code offset} is an offset into the entire list of results; {@code howMany} is the desired
 * number of results to return from there. For example, {@code offset=30} and {@code howMany=5}
 * will cause the implementation to retrieve 35 results internally and output the last 5.
 * If {@code howMany} is not specified, defaults to {@link AbstractALSServlet#DEFAULT_HOW_MANY}.
 * {@code offset} defaults to 0.</p>
 *
 * <p>Unknown item IDs are ignored, unless all are unknown, in which case a
 * {@link HttpServletResponse#SC_BAD_REQUEST} status is returned.</p>
 *
 * <p>Outputs item/score pairs like {@link RecommendServlet} does.</p>
 *
 * <p>This does something slightly different from {@link com.cloudera.oryx.als.serving.web.SimilarityServlet};
 * see {@link OryxRecommender#recommendToAnonymous(String[], float[], int)}.</p>
 *
 * @author Sean Owen
 * @see com.cloudera.oryx.als.serving.web.SimilarityServlet
 */
public final class RecommendToAnonymousServlet extends AbstractALSServlet {
  
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {

    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    Iterator<String> pathComponents = SLASH.split(pathInfo).iterator();
    Pair<String[],float[]> itemIDsAndValue;
    try {
      itemIDsAndValue = parseItemValuePairs(pathComponents);
    } catch (NoSuchElementException nsee) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, nsee.toString());
      return;
    }

    if (itemIDsAndValue.getFirst().length == 0) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No items");
      return;
    }

    String[] itemIDs = itemIDsAndValue.getFirst();
    float[] values = itemIDsAndValue.getSecond();
    
    OryxRecommender recommender = getRecommender();
    RescorerProvider rescorerProvider = getRescorerProvider();
    try {
      Rescorer rescorer = rescorerProvider == null ? null :
          rescorerProvider.getRecommendToAnonymousRescorer(itemIDs, recommender, getRescorerParams(request));
      output(request,
             response,
             recommender.recommendToAnonymous(itemIDs, values, getNumResultsToFetch(request), rescorer));
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    } catch (NoSuchItemException nsie) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, nsie.toString());
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, iae.toString());
    }
  }
  
  static Pair<String[],float[]> parseItemValuePairs(Iterator<String> pathComponents) {
    List<Pair<String,Float>> itemValuePairs = new ArrayList<>(1);
    while (pathComponents.hasNext()) {
      itemValuePairs.add(parseItemValue(pathComponents.next()));
    }
    
    int size = itemValuePairs.size();
    String[] itemIDs = new String[size];
    float[] values = new float[size];
    for (int i = 0; i < size; i++) {
      Pair<String,Float> itemValuePair = itemValuePairs.get(i);
      itemIDs[i] = itemValuePair.getFirst();
      Float value = itemValuePair.getSecond();
      values[i] = value == null ? 1.0f : value;
    }
    
    return new Pair<>(itemIDs, values);
  }

  private static Pair<String,Float> parseItemValue(String s) {
    int equals = s.indexOf('=');
    if (equals < 0) {
      return new Pair<>(s, null);
    }
    return new Pair<>(s.substring(0, equals), LangUtils.parseFloat(s.substring(equals + 1)));
  }

}
