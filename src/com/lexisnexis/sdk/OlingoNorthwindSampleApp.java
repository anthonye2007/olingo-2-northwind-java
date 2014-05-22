/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 ******************************************************************************/
package com.lexisnexis.sdk;

import org.apache.olingo.odata2.api.commons.HttpStatusCodes;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntityContainer;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.api.ep.feed.ODataDeltaFeed;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;
import org.apache.olingo.odata2.api.exception.ODataException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Demonstrates the usage of the Apache Olingo v2 client library to access the Microsoft sample Northwind database.
 */
public class OlingoNorthwindSampleApp {
    public static final String HTTP_METHOD_PUT = "PUT";
    public static final String HTTP_METHOD_POST = "POST";
    public static final String HTTP_METHOD_GET = "GET";
    public static final String HTTP_HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HTTP_HEADER_ACCEPT = "Accept";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_XML = "application/xml";
    //public static final String APPLICATION_ATOM_XML = "application/atom+xml";
    //public static final String APPLICATION_FORM = "application/x-www-form-urlencoded";
    public static final String METADATA = "$metadata";
    public static final String SEPARATOR = "/";
    public static final boolean PRINT_RAW_CONTENT = true;

    public static void main(String[] paras) throws Exception {
        OlingoNorthwindSampleApp app = new OlingoNorthwindSampleApp();

        String serviceUrl = "http://services.odata.org/V2/Northwind/Northwind.svc";
        String usedFormat = APPLICATION_JSON;

        print("\n----- Read Edm ------------------------------");
        Edm edm = app.readEdm(serviceUrl);
        print("Read default EntityContainer: " + edm.getDefaultEntityContainer().getName());

        print("\n----- Read Feed ------------------------------");
        ODataFeed feed = app.readFeed(edm, serviceUrl, usedFormat, "Customers");

        print("Read: " + feed.getEntries().size() + " entries: ");
        for (ODataEntry entry : feed.getEntries()) {
            print("##########");
            print("Entry:\n" + prettyPrint(entry));
            print("##########");
        }

        print("\n----- Read Entry ------------------------------");
        String customerId = "ERNSH";
        String formattedCustomerId = "'" + customerId + "'";
        ODataEntry entry = app.readEntry(edm, serviceUrl, usedFormat, "Customers", formattedCustomerId);
        print("Single Entry:\n" + prettyPrint(entry));

        print("\n----- Read Entry with $expand  ------------------------------");
        ODataEntry entryExpanded = app.readEntry(edm, serviceUrl, usedFormat, "Orders", "10248", "Customer");
        print("Single Entry with expanded Customer relation:\n" + prettyPrint(entryExpanded));
    }

    private static void print(String content) {
        System.out.println(content);
    }

    private static String prettyPrint(ODataEntry createdEntry) {
        return prettyPrint(createdEntry.getProperties(), 0);
    }

    private static String prettyPrint(Map<String, Object> properties, int level) {
        StringBuilder b = new StringBuilder();
        Set<Entry<String, Object>> entries = properties.entrySet();

        for (Entry<String, Object> entry : entries) {
            intend(b, level);
            b.append(entry.getKey()).append(": ");
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = prettyPrint((Map<String, Object>) value, level + 1);
                b.append(value).append("\n");
            } else if (value instanceof Calendar) {
                Calendar cal = (Calendar) value;
                value = SimpleDateFormat.getInstance().format(cal.getTime());
                b.append(value).append("\n");
            } else if (value instanceof ODataDeltaFeed) {
                ODataDeltaFeed feed = (ODataDeltaFeed) value;
                List<ODataEntry> inlineEntries = feed.getEntries();
                b.append("{");
                for (ODataEntry oDataEntry : inlineEntries) {
                    value = prettyPrint(oDataEntry.getProperties(), level + 1);
                    b.append("\n[\n").append(value).append("\n],");
                }
                b.deleteCharAt(b.length() - 1);
                intend(b, level);
                b.append("}\n");
            } else {
                b.append(value).append("\n");
            }
        }
        // remove last line break
        b.deleteCharAt(b.length() - 1);
        return b.toString();
    }

    private static void intend(StringBuilder builder, int intendLevel) {
        for (int i = 0; i < intendLevel; i++) {
            builder.append("  ");
        }
    }

    public Edm readEdm(String serviceUrl) throws IOException, ODataException {
        InputStream content = execute(serviceUrl + SEPARATOR + METADATA, APPLICATION_XML, HTTP_METHOD_GET);
        return EntityProvider.readMetadata(content, false);
    }

    public ODataFeed readFeed(Edm edm, String serviceUri, String contentType, String entitySetName)
            throws IOException, ODataException {
        EdmEntityContainer entityContainer = edm.getDefaultEntityContainer();
        String absoluteUri = createUri(serviceUri, entitySetName, null);

        InputStream content = execute(absoluteUri, contentType, HTTP_METHOD_GET);
        return EntityProvider.readFeed(contentType,
                entityContainer.getEntitySet(entitySetName),
                content,
                EntityProviderReadProperties.init().build());
    }

    public ODataEntry readEntry(Edm edm, String serviceUri, String contentType, String entitySetName, String keyValue)
            throws IOException, ODataException {
        return readEntry(edm, serviceUri, contentType, entitySetName, keyValue, null);
    }

    public ODataEntry readEntry(Edm edm, String serviceUri, String contentType,
                                String entitySetName, String keyValue, String expandRelationName)
            throws IOException, ODataException {
        // working with the default entity container
        EdmEntityContainer entityContainer = edm.getDefaultEntityContainer();
        // create absolute uri based on service uri, entity set name with its key property value and optional expanded relation name
        String absoluteUri = createUri(serviceUri, entitySetName, keyValue, expandRelationName);

        InputStream content = execute(absoluteUri, contentType, HTTP_METHOD_GET);

        return EntityProvider.readEntry(contentType,
                entityContainer.getEntitySet(entitySetName),
                content,
                EntityProviderReadProperties.init().build());
    }

    private InputStream logRawContent(String prefix, InputStream content, String postfix) throws IOException {
        if (PRINT_RAW_CONTENT) {
            byte[] buffer = streamToArray(content);
            print(prefix + new String(buffer) + postfix);
            return new ByteArrayInputStream(buffer);
        }
        return content;
    }

    private byte[] streamToArray(InputStream stream) throws IOException {
        byte[] result = new byte[0];
        byte[] tmp = new byte[8192];
        int readCount = stream.read(tmp);
        while (readCount >= 0) {
            byte[] innerTmp = new byte[result.length + readCount];
            System.arraycopy(result, 0, innerTmp, 0, result.length);
            System.arraycopy(tmp, 0, innerTmp, result.length, readCount);
            result = innerTmp;
            readCount = stream.read(tmp);
        }
        stream.close();
        return result;
    }

    private HttpStatusCodes checkStatus(HttpURLConnection connection) throws IOException {
        HttpStatusCodes httpStatusCode = HttpStatusCodes.fromStatusCode(connection.getResponseCode());
        if (400 <= httpStatusCode.getStatusCode() && httpStatusCode.getStatusCode() <= 599) {
            throw new RuntimeException("Http Connection failed with status " + httpStatusCode.getStatusCode() + " " + httpStatusCode.toString());
        }
        return httpStatusCode;
    }

    private String createUri(String serviceUri, String entitySetName, String id) {
        return createUri(serviceUri, entitySetName, id, null);
    }

    private String createUri(String serviceUri, String entitySetName, String id, String expand) {
        final StringBuilder absoluteUri = new StringBuilder(serviceUri).append(SEPARATOR).append(entitySetName);
        if (id != null) {
            absoluteUri.append("(").append(id).append(")");
        }
        if (expand != null) {
            absoluteUri.append("/?$expand=").append(expand);
        }
        return absoluteUri.toString();
    }

    private InputStream execute(String relativeUri, String contentType, String httpMethod) throws IOException {
        HttpURLConnection connection = initializeConnection(relativeUri, contentType, httpMethod);

        connection.connect();
        checkStatus(connection);

        InputStream content = connection.getInputStream();
        content = logRawContent(httpMethod + " request on uri '" + relativeUri + "' with content:\n  ", content, "\n");
        return content;
    }

    private HttpURLConnection initializeConnection(String absoluteUri, String contentType, String httpMethod)
            throws IOException {
        URL url = new URL(absoluteUri);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod(httpMethod);
        connection.setRequestProperty(HTTP_HEADER_ACCEPT, contentType);
        if (HTTP_METHOD_POST.equals(httpMethod) || HTTP_METHOD_PUT.equals(httpMethod)) {
            connection.setDoOutput(true);
            connection.setRequestProperty(HTTP_HEADER_CONTENT_TYPE, contentType);
        }

        return connection;
    }
}