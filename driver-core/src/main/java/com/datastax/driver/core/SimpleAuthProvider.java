/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import javax.security.sasl.SaslException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple {@code AuthProvider} implementation.
 * <p>
 * This provider allows to programmatically define authentication
 * information that will then apply to all hosts. The
 * SimpleAuthenticator instances it returns support SASL
 * authentication using the PLAIN mechanism for version 2 of the
 * CQL native protocol as well as legacy username/password
 * authentication for protocol version 1
 *
 * <p>
 * Note that it is <b>not</b> safe to add new information to this provider once a
 * Cluster instance has been created using this provider.
 */
public class SimpleAuthProvider implements AuthProvider {
    private final Map<String, String> credentials = new HashMap<String, String>();

    /**
     * Creates a new simple authentication information provider with the
     * informations contained in {@code properties}.
     *
     * @param properties a map of authentication information to use.
     */
    public SimpleAuthProvider(Map<String, String> properties) {
        addAll(properties);
    }

    /**
     * Adds a new property to the authentication information returned by this
     * provider.
     *
     * @param property the name of the property to add.
     * @param value the value to add for {@code property}.
     * @return {@code this} object.
     */
    public SimpleAuthProvider add(String property, String value) {
        credentials.put(property, value);
        return this;
    }

    /**
     * Adds all the key-value pair provided as new authentication
     * information returned by this provider.
     *
     * @param properties a map of authentication information to add.
     * @return {@code this} object.
     */
    public SimpleAuthProvider addAll(Map<String, String> properties) {
        credentials.putAll(properties);
        return this;
    }

    /**
     * Uses the credentials in the supplied map to initialise a
     * SaslAuthenticator which uses the SASL PLAIN mechanism to login
     * with the server. For backwards compatibility with version 1 of
     * the protocol, SimpleAuthenticator will respond to a v1
     * AuthenticateMessage with a CredentialsMessage.
     *
     * @param host the Cassandra host with which we want to authenticate
     * @return an Authenticator instance which can be used to perform
     * authentication negotiations on behalf of the client
     * @throws SaslException if an unsupported SASL mechanism is supplied
     * or an error is encountered when initialising the authenticator
     */
    public Authenticator newAuthenticator(InetAddress host) {
        return new SimpleAuthenticator(credentials);
    }
}
