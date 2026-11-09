package ru.demo.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ChatWebSocketHandler extends TextWebSocketHandler {
    private static final int MAX_MESSAGE_LENGTH = 1000;
    private static final int MAX_HISTORY = 50;

    private final Map<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    private final List<ChatMessage> history = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper;

    public ChatWebSocketHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws IOException {
        sessions.put(session.getId(), session);
        send(session, new ChatMessage("system", "Система",
                "Соединение установлено. Сейчас онлайн: " + sessions.size(), Instant.now()));
        for (ChatMessage message : history) {
            send(session, message);
        }
        broadcast(new ChatMessage("system", "Система",
                "Новый участник подключился. Онлайн: " + sessions.size(), Instant.now()));
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        String text = message.getPayload().trim();
        if (text.isEmpty()) {
            return;
        }
        if (text.length() > MAX_MESSAGE_LENGTH) {
            send(session, new ChatMessage("error", "Система",
                    "Сообщение должно быть не длиннее " + MAX_MESSAGE_LENGTH + " символов.", Instant.now()));
            return;
        }

        String sender = session.getPrincipal() == null ? "Гость-" + session.getId().substring(0, 6)
                : session.getPrincipal().getName();
        ChatMessage chatMessage = new ChatMessage("message", sender, text, Instant.now());
        history.add(chatMessage);
        while (history.size() > MAX_HISTORY) {
            history.remove(0);
        }
        broadcast(chatMessage);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        sessions.remove(session.getId());
        broadcast(new ChatMessage("system", "Система",
                "Участник отключился. Онлайн: " + sessions.size(), Instant.now()));
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        sessions.remove(session.getId());
        if (session.isOpen()) {
            session.close(CloseStatus.SERVER_ERROR);
        }
    }

    private void broadcast(ChatMessage message) {
        String json;
        try {
            json = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Не удалось сериализ��вать сообщение чата", exception);
        }

        for (WebSocketSession client : sessions.values()) {
            try {
                synchronized (client) {
                    if (client.isOpen()) {
                        client.sendMessage(new TextMessage(json));
                    }
                }
            } catch (IOException exception) {
                sessions.remove(client.getId());
                try {
                    client.close(CloseStatus.SERVER_ERROR);
                } catch (IOException ignored) {
                    // Соединение уже оборвано.
                }
            }
        }
    }

    private void send(WebSocketSession session, ChatMessage message) throws IOException {
        String json;
        try {
            json = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Не удалось сериализовать сообщение чата", exception);
        }
        synchronized (session) {
            if (session.isOpen()) {
                session.sendMessage(new TextMessage(json));
            }
        }
    }

    public List<ChatMessage> recentMessages() {
        return new ArrayList<>(history);
    }
}
