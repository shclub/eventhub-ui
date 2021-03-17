package eu.adrianistan

import com.azure.messaging.eventhubs.CheckpointStore
import com.azure.messaging.eventhubs.models.PartitionOwnership
import java.util.concurrent.ConcurrentHashMap
import com.azure.messaging.eventhubs.models.Checkpoint
import com.azure.core.util.logging.ClientLogger
import eu.adrianistan.NullCheckpointStore
import reactor.core.publisher.Flux
import java.lang.StringBuilder
import java.util.Locale
import com.azure.core.util.CoreUtils
import java.util.UUID
import reactor.core.publisher.Mono
import java.lang.Void
import java.lang.NullPointerException

/* Taken from https://github.com/Azure/azure-sdk-for-java/blob/master/sdk/eventhubs/azure-messaging-eventhubs/src/samples/java/com/azure/messaging/eventhubs/SampleCheckpointStore.java */ /* Code under the MIT License */
/**
 * A simple in-memory implementation of a [CheckpointStore]. This implementation keeps track of partition
 * ownership details including checkpointing information in-memory. Using this implementation will only facilitate
 * checkpointing and load balancing of Event Processors running within this process.
 */
class NullCheckpointStore : CheckpointStore {
    private val partitionOwnershipMap: MutableMap<String, PartitionOwnership> = ConcurrentHashMap()
    private val checkpointsMap: MutableMap<String, Checkpoint> = ConcurrentHashMap()
    private val logger = ClientLogger(NullCheckpointStore::class.java)

    /**
     * {@inheritDoc}
     */
    override fun listOwnership(
        fullyQualifiedNamespace: String, eventHubName: String,
        consumerGroup: String
    ): Flux<PartitionOwnership> {
        logger.info("Listing partition ownership")
        val prefix = prefixBuilder(fullyQualifiedNamespace, eventHubName, consumerGroup, OWNERSHIP)
        return Flux.fromIterable(partitionOwnershipMap.keys)
            .filter { key: String -> key.startsWith(prefix) }
            .map { key: String -> partitionOwnershipMap[key] }
    }

    private fun prefixBuilder(
        fullyQualifiedNamespace: String, eventHubName: String, consumerGroup: String,
        type: String
    ): String {
        return StringBuilder()
            .append(fullyQualifiedNamespace)
            .append(SEPARATOR)
            .append(eventHubName)
            .append(SEPARATOR)
            .append(consumerGroup)
            .append(SEPARATOR)
            .append(type)
            .toString()
            .toLowerCase(Locale.ROOT)
    }

    /**
     * Returns a [Flux] of partition ownership details for successfully claimed partitions. If a partition is
     * already claimed by an instance or if the ETag in the request doesn't match the previously stored ETag, then
     * ownership claim is denied.
     *
     * @param requestedPartitionOwnerships List of partition ownerships this instance is requesting to own.
     * @return Successfully claimed partition ownerships.
     */
    override fun claimOwnership(requestedPartitionOwnerships: List<PartitionOwnership>): Flux<PartitionOwnership> {
        if (CoreUtils.isNullOrEmpty(requestedPartitionOwnerships)) {
            return Flux.empty()
        }
        val firstEntry = requestedPartitionOwnerships[0]
        val prefix = prefixBuilder(
            firstEntry.fullyQualifiedNamespace, firstEntry.eventHubName,
            firstEntry.consumerGroup, OWNERSHIP
        )
        return Flux.fromIterable(requestedPartitionOwnerships)
            .filter { partitionOwnership: PartitionOwnership ->
                (!partitionOwnershipMap.containsKey(partitionOwnership.partitionId)
                        || (partitionOwnershipMap[partitionOwnership.partitionId]!!.eTag
                        == partitionOwnership.eTag))
            }
            .doOnNext { partitionOwnership: PartitionOwnership ->
                logger
                    .info(
                        "Ownership of partition {} claimed by {}", partitionOwnership.partitionId,
                        partitionOwnership.ownerId
                    )
            }
            .map { partitionOwnership: PartitionOwnership ->
                partitionOwnership.setETag(UUID.randomUUID().toString()).lastModifiedTime = System.currentTimeMillis()
                partitionOwnershipMap[prefix + SEPARATOR + partitionOwnership.partitionId] = partitionOwnership
                partitionOwnership
            }
    }

    /**
     * {@inheritDoc}
     */
    override fun listCheckpoints(
        fullyQualifiedNamespace: String, eventHubName: String,
        consumerGroup: String
    ): Flux<Checkpoint> {
        val prefix = prefixBuilder(fullyQualifiedNamespace, eventHubName, consumerGroup, CHECKPOINT)
        return Flux.fromIterable(checkpointsMap.keys)
            .filter { key: String -> key.startsWith(prefix) }
            .map { key: String -> checkpointsMap[key] }
    }

    /**
     * Updates the in-memory storage with the provided checkpoint information.
     *
     * @param checkpoint The checkpoint containing the information to be stored in-memory.
     * @return A [Mono] that completes when the checkpoint is updated.
     */
    override fun updateCheckpoint(checkpoint: Checkpoint): Mono<Void> {
        if (checkpoint == null) {
            return Mono.error(logger.logExceptionAsError(NullPointerException("checkpoint cannot be null")))
        }
        val prefix = prefixBuilder(
            checkpoint.fullyQualifiedNamespace, checkpoint.eventHubName,
            checkpoint.consumerGroup, CHECKPOINT
        )
        checkpointsMap[prefix + SEPARATOR + checkpoint.partitionId] = checkpoint
        logger.info(
            "Updated checkpoint for partition {} with sequence number {}", checkpoint.partitionId,
            checkpoint.sequenceNumber
        )
        return Mono.empty()
    }

    companion object {
        private const val OWNERSHIP = "ownership"
        private const val SEPARATOR = "/"
        private const val CHECKPOINT = "checkpoint"
    }
}