package eu.adrianistan

import java.time.Instant

data class EventHubMessage(
    val partition: String,
    val sequence: Long,
    val body: String,
    val timestamp: Instant,
    val properties: List<EventHubProperty>
)