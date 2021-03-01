package eu.adrianistan;

public class EventHubMessage {
    private final String partition;
    private final Long sequence;
    private final String body;

    public EventHubMessage(String partition, Long sequence, String body) {
        this.partition = partition;
        this.sequence = sequence;
        this.body = body;
    }

    public String getPartition(){
        return partition;
    }

    public Long getSequence(){
        return sequence;
    }

    public String getBody(){
        return body;
    }
}