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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.google.common.net.HttpHeaders;

import com.cloudera.oryx.als.common.OryxRecommender;

/**
 * <p>Responds to a POST request to {@code /ingest} and in turn calls
 * {@link OryxRecommender#ingest(Reader)}}. The content of the request body is
 * fed to this method. Note that the content may be gzipped; if so, header "Content-Encoding"
 * must have value "gzip".</p>
 *
 * <p>Alternatively, CSV data may be POSTed here as if part of a web browser file upload. In this case
 * the "Content-Type" should be "multipart/form-data", and the payload encoded accordingly. The uploaded
 * file may be gzipped or zipped.</p>
 *
 * @author Sean Owen
 */
public final class IngestServlet extends AbstractALSServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    OryxRecommender recommender = getRecommender();

    String contentType = request.getContentType();
    boolean fromBrowserUpload = contentType != null && contentType.startsWith("multipart/form-data");

    Reader reader;
    if (fromBrowserUpload) {

      Collection<Part> parts = request.getParts();
      if (parts == null || parts.isEmpty()) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No form data");
        return;
      }
      Part part = parts.iterator().next();
      String partContentType = part.getContentType();
      InputStream in = part.getInputStream();
      switch (partContentType) {
        case "application/zip":
          in = new ZipInputStream(in);
          break;
        case "application/gzip":
          in = new GZIPInputStream(in);
          break;
        case "application/x-gzip":
          in = new GZIPInputStream(in);
          break;
      }
      reader = new InputStreamReader(in, StandardCharsets.UTF_8);

    } else {

      String charEncodingName = request.getCharacterEncoding();
      Charset charEncoding = charEncodingName == null ? StandardCharsets.UTF_8 : Charset.forName(charEncodingName);
      String contentEncoding = request.getHeader(HttpHeaders.CONTENT_ENCODING);
      if (contentEncoding == null) {
        reader = request.getReader();
      } else if ("gzip".equals(contentEncoding)) {
        reader = new InputStreamReader(new GZIPInputStream(request.getInputStream()), charEncoding);
      } else if ("zip".equals(contentEncoding)) {
        reader = new InputStreamReader(new ZipInputStream(request.getInputStream()), charEncoding);
      } else {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unsupported Content-Encoding");
        return;
      }

    }

    try {
      recommender.ingest(reader);
    } catch (IllegalArgumentException | NoSuchElementException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, iae.toString());
      return;
    }

    String referer = request.getHeader(HttpHeaders.REFERER);
    if (fromBrowserUpload && referer != null) {
      // Parsing avoids response splitting
      response.sendRedirect(new URL(referer).toString());
    }

  }

}
