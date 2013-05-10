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

import org.apache.cassandra.transport.messages.AuthenticateMessage;

import java.util.concurrent.ExecutionException;

/**
 * Handles authentication with Cassandra servers.
 *
 * A server which requires authentication responds to a startup
 * message with an challenge in the form of an {@code AuthenticateMessage}.
 * Authenticator implementations should be able to respond to that
 * challenge and perform whatever authentication negotiation is required
 * by the server. The exact nature of that negotiation is specific to the
 * configuration of the server.
 */
public interface Authenticator {
    /**
     * Respond to an authentication challenge sent by the server. How this
     * is handled depends on the particular authentication scheme employed
     * by the server.
     *
     * @param message the authentication challenge received from the server
     * @param connection used to send response message(s) to the server
     * @throws ConnectionException
     * @throws BusyConnectionException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void handleAuthenticationRequest(AuthenticateMessage message, Connection connection)
            throws ConnectionException, BusyConnectionException, ExecutionException, InterruptedException;
}
