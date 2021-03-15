package eu.adrianistan

import javafx.fxml.FXML
import eu.adrianistan.EventHubMessage
import javafx.collections.ObservableList
import javafx.collections.FXCollections
import com.azure.messaging.eventhubs.EventProcessorClient
import javafx.scene.control.cell.PropertyValueFactory
import javafx.beans.value.ObservableValue
import com.azure.messaging.eventhubs.EventProcessorClientBuilder
import com.azure.messaging.eventhubs.EventHubClientBuilder
import eu.adrianistan.NullCheckpointStore
import com.azure.messaging.eventhubs.models.ErrorContext
import com.azure.messaging.eventhubs.models.EventContext
import javafx.event.ActionEvent
import javafx.scene.control.*
import java.util.function.Consumer

class MainController {
    @FXML
    var table: TableView<EventHubMessage>? = null

    @FXML
    var partitionCol: TableColumn<*, *>? = null

    @FXML
    var sequenceCol: TableColumn<*, *>? = null

    @FXML
    var bodyCol: TableColumn<*, *>? = null

    @FXML
    var connectionString: TextField? = null

    @FXML
    var hubName: TextField? = null

    @FXML
    var connect: Button? = null

    @FXML
    var msg: TextArea? = null
    private val data = FXCollections.observableArrayList<EventHubMessage>()
    private var eventProcessorClient: EventProcessorClient? = null
    fun initialize() {
        partitionCol!!.cellValueFactory = PropertyValueFactory<EventHubMessage, String>("partition")
        sequenceCol!!.cellValueFactory = PropertyValueFactory<EventHubMessage, Long>("sequence")
        bodyCol!!.cellValueFactory = PropertyValueFactory<EventHubMessage, String>("body")
        table!!.items = data
        table!!.selectionModel.selectionMode = SelectionMode.SINGLE
        table!!.selectionModel.isCellSelectionEnabled = true
        table!!.selectionModel.selectedItemProperty()
            .addListener { obs: ObservableValue<out EventHubMessage>?, oldSelection: EventHubMessage?, newSelection: EventHubMessage? ->
                if (newSelection != null) {
                    msg!!.text = newSelection.body
                }
            }
    }

    @FXML
    fun handleDoConnect(event: ActionEvent?) {
        if (eventProcessorClient == null) {
            val eventProcessorClientBuilder = EventProcessorClientBuilder()
                .connectionString(connectionString!!.text, hubName!!.text)
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .processEvent(processEvent)
                .processError(processError)
                .checkpointStore(NullCheckpointStore())
            eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient()
            eventProcessorClient?.let {
                it.start()
                connect!!.text = "Close connection"
            }
        } else {
            eventProcessorClient!!.stop()
            eventProcessorClient = null
            connect!!.text = "Connect"
        }
    }

    val processEvent = Consumer { eventContext: EventContext ->
        System.out.printf("Message received: %s \n", eventContext.eventData.bodyAsString)
        data.add(
            EventHubMessage(
                eventContext.partitionContext.partitionId,
                eventContext.eventData.sequenceNumber,
                eventContext.eventData.bodyAsString
            )
        )
    }
    val processError = Consumer { errorContext: ErrorContext ->
        System.out.printf(
            "Error occurred in partition processor for partition %s, %s.%n",
            errorContext.partitionContext.partitionId,
            errorContext.throwable
        )
    }
}