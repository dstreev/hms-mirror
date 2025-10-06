/*
 * Copyright (c) 2024-2025. Cloudera, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.cloudera.utils.hms.mirror.service;

import com.cloudera.utils.hms.mirror.domain.HmsMirrorConfig;
import com.cloudera.utils.hms.mirror.domain.support.ExecuteSession;
import com.cloudera.utils.hms.mirror.exceptions.SessionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SessionManagerTest {

    @Mock
    private ExecuteSessionService executeSessionService;

    private SessionManager sessionManager;
    private MockHttpSession httpSession;
    private MockHttpServletRequest request;

    @BeforeEach
    void setUp() {
        sessionManager = new SessionManager(executeSessionService);
        httpSession = new MockHttpSession();
        request = new MockHttpServletRequest();
        request.setSession(httpSession);
        
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    @Test
    void testCreateSession() throws SessionException {
        // Given
        String sessionId = "test-session";
        HmsMirrorConfig config = new HmsMirrorConfig();
        ExecuteSession expectedSession = new ExecuteSession();
        expectedSession.setSessionId(sessionId);
        
        when(executeSessionService.createSession(sessionId, config)).thenReturn(expectedSession);

        // When
        ExecuteSession result = sessionManager.createSession(sessionId, config);

        // Then
        assertNotNull(result);
        assertEquals(sessionId, result.getSessionId());
        assertTrue(sessionManager.isSessionExists(sessionId));
    }

    @Test
    void testGetCurrentSessionFromHttpSession() throws SessionException {
        // Given
        String sessionId = "web-session";
        ExecuteSession session = new ExecuteSession();
        session.setSessionId(sessionId);
        
        when(executeSessionService.createSession(sessionId, null)).thenReturn(session);
        
        // When
        sessionManager.createSession(sessionId, null);
        ExecuteSession result = sessionManager.getCurrentSession();

        // Then
        assertNotNull(result);
        assertEquals(sessionId, result.getSessionId());
    }

    @Test
    void testGetCurrentSessionWithoutHttpSession() {
        // Given - clear the request context
        RequestContextHolder.setRequestAttributes(null);
        ExecuteSession defaultSession = new ExecuteSession();
        defaultSession.setSessionId(SessionManager.DEFAULT_SESSION_ID);
        
        when(executeSessionService.getSession()).thenReturn(defaultSession);

        // When
        ExecuteSession result = sessionManager.getCurrentSession();

        // Then
        assertNotNull(result);
        assertEquals(SessionManager.DEFAULT_SESSION_ID, result.getSessionId());
    }

    @Test
    void testGetDefaultSession() {
        // Given
        ExecuteSession defaultSession = new ExecuteSession();
        defaultSession.setSessionId(SessionManager.DEFAULT_SESSION_ID);
        
        when(executeSessionService.getOrCreateDefaultSession()).thenReturn(defaultSession);

        // When
        ExecuteSession result = sessionManager.getDefaultSession();

        // Then
        assertNotNull(result);
        assertEquals(SessionManager.DEFAULT_SESSION_ID, result.getSessionId());
    }

    @Test
    void testCreateDefaultSessionWhenNull() throws SessionException {
        // Given
        when(executeSessionService.getOrCreateDefaultSession()).thenReturn(null);
        ExecuteSession defaultSession = new ExecuteSession();
        defaultSession.setSessionId(SessionManager.DEFAULT_SESSION_ID);
        when(executeSessionService.createSession(SessionManager.DEFAULT_SESSION_ID, null)).thenReturn(defaultSession);

        // When
        ExecuteSession result = sessionManager.getDefaultSession();

        // Then
        assertNotNull(result);
        assertEquals(SessionManager.DEFAULT_SESSION_ID, result.getSessionId());
    }

    @Test
    void testGetSessionById() throws SessionException {
        // Given
        String sessionId = "specific-session";
        HmsMirrorConfig config = new HmsMirrorConfig();
        ExecuteSession session = new ExecuteSession();
        session.setSessionId(sessionId);
        
        when(executeSessionService.createSession(sessionId, config)).thenReturn(session);

        // When
        sessionManager.createSession(sessionId, config);
        ExecuteSession result = sessionManager.getCurrentSession(sessionId);

        // Then
        assertNotNull(result);
        assertEquals(sessionId, result.getSessionId());
    }

    @Test
    void testCloseSession() throws SessionException {
        // Given
        String sessionId = "session-to-close";
        HmsMirrorConfig config = new HmsMirrorConfig();
        ExecuteSession session = new ExecuteSession();
        session.setSessionId(sessionId);
        
        when(executeSessionService.createSession(sessionId, config)).thenReturn(session);

        // When
        sessionManager.createSession(sessionId, config);
        assertTrue(sessionManager.isSessionExists(sessionId));
        
        sessionManager.closeSession(sessionId);

        // Then
        assertFalse(sessionManager.isSessionExists(sessionId));
    }

    @Test
    void testGetAllSessions() throws SessionException {
        // Given
        String session1Id = "session1";
        String session2Id = "session2";
        
        ExecuteSession session1 = new ExecuteSession();
        session1.setSessionId(session1Id);
        ExecuteSession session2 = new ExecuteSession();
        session2.setSessionId(session2Id);
        ExecuteSession defaultSession = new ExecuteSession();
        defaultSession.setSessionId(SessionManager.DEFAULT_SESSION_ID);
        
        when(executeSessionService.createSession(session1Id, null)).thenReturn(session1);
        when(executeSessionService.createSession(session2Id, null)).thenReturn(session2);
        when(executeSessionService.getOrCreateDefaultSession()).thenReturn(defaultSession);

        // When
        sessionManager.createSession(session1Id, null);
        sessionManager.createSession(session2Id, null);
        sessionManager.getDefaultSession(); // Initialize default session
        Map<String, ExecuteSession> allSessions = sessionManager.getAllSessions();

        // Then
        assertEquals(3, allSessions.size());
        assertTrue(allSessions.containsKey(session1Id));
        assertTrue(allSessions.containsKey(session2Id));
        assertTrue(allSessions.containsKey(SessionManager.DEFAULT_SESSION_ID));
    }

    @Test
    void testIsSessionExists() throws SessionException {
        // Given
        String sessionId = "existing-session";
        ExecuteSession session = new ExecuteSession();
        session.setSessionId(sessionId);
        
        when(executeSessionService.createSession(sessionId, null)).thenReturn(session);

        // When
        assertFalse(sessionManager.isSessionExists(sessionId));
        sessionManager.createSession(sessionId, null);

        // Then
        assertTrue(sessionManager.isSessionExists(sessionId));
    }
}