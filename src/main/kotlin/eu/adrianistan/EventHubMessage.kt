package eu.adrianistan

class EventHubMessage(val partition: String, val sequence: Long, val body: String)