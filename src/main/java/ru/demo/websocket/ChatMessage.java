package ru.demo.websocket;

import java.time.Instant;

public record ChatMessage(String type, String sender, String text, Instant sentAt) {
}
