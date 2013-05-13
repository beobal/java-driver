/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import org.apache.cassandra.auth.IAuthenticator;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Default implementation of {@link Authenticator} which can
 * perform authentication against Cassandra servers configured
 * with PasswordAuthenticator (or another IAuthenticator implementation
 * with the same interface).
 *
 * It supports SASL authentication using the PLAIN mechanism for version
 * 2 of the CQL native protocol as well as legacy username/password
 * authentication for protocol version 1
 */
public class SimpleAuthenticator implements Authenticator {

    private final Map<String, String> credentials;

    public SimpleAuthenticator(Map<String, String> credentials) {
        this.credentials = credentials;
    }

    @Override
    public byte[] initialResponse() {
        byte[] username = credentials.get(IAuthenticator.USERNAME_KEY).getBytes(Charset.forName("UTF-8"));
        byte[] password = credentials.get(IAuthenticator.PASSWORD_KEY).getBytes(Charset.forName("UTF-8"));
        byte[] initialToken = new byte[username.length + password.length + 2];
        initialToken[0] = 0;
        System.arraycopy(username, 0, initialToken, 1, username.length);
        initialToken[username.length + 1] = 0;
        System.arraycopy(password, 0, initialToken, username.length + 2, password.length);
        return initialToken;
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) {
        return null;
    }
}
