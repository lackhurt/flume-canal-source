/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.weiboyi.etl.flume.source.canal;

import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.StringUtils;
import sun.net.util.IPAddressUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class CanalConf {

    private String zkServers;
    private String destination;
    private String username;
    private String password;
    private int batchSize;
    private String serverUrl;
    private String serverUrls;
    private String filter;
    private Boolean oldDataRequired;

    public String getZkServers() {
        return zkServers;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public void setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String getServerUrls() {
        return serverUrls;
    }

    public void setServerUrls(String serverUrls) {
        this.serverUrls = serverUrls;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Boolean getOldDataRequired() {
        return oldDataRequired;
    }

    public void setOldDataRequired(Boolean oldDataRequired) {
        this.oldDataRequired = oldDataRequired;
    }

    public boolean isConnectionUrlValid() {
        if (isNullOrEmpty(this.zkServers)
                && isNullOrEmpty(this.serverUrl)
                && isNullOrEmpty(this.serverUrls)) {
            return false;
        } else {
            return true;
        }
    }

    public static List<SocketAddress> convertUrlsToSocketAddressList(String serverUrls) throws ServerUrlsFormatException {
        List<SocketAddress> addresses = new ArrayList<>();

        if (StringUtils.isNotEmpty(serverUrls)) {
            for (String serverUrl : serverUrls.split(",")) {
                if (StringUtils.isNotEmpty(serverUrl)) {
                    try {
                        addresses.add(convertUrlToSocketAddress(serverUrl));
                    } catch (Exception exception) {
                        throw new ServerUrlsFormatException(String.format("The serverUrls are malformed . The ServerUrls : \"%s\" .", serverUrls), exception);
                    }
                }
            }
            return addresses;
        } else {
            return addresses;
        }
    }

    public static SocketAddress convertUrlToSocketAddress(String serverUrl) throws ServerUrlsFormatException, NumberFormatException {

        String[] hostAndPort = serverUrl.split(":");

        if (hostAndPort.length == 2 && StringUtils.isNotEmpty(hostAndPort[1])) {
            try {

                int port  = Integer.parseInt(hostAndPort[1]);
                InetSocketAddress socketAddress = new InetSocketAddress(hostAndPort[0], port);
                return socketAddress;

            } catch (NumberFormatException exception) {
                throw exception;
            }
        } else {
            throw new ServerUrlsFormatException(String.format("The serverUrl is malformed . The ServerUrl : \"%s\" .", serverUrl));
        }
    }

    private boolean isNullOrEmpty(String value) {
        return value == null || "".equals(value.trim());
    }
}
