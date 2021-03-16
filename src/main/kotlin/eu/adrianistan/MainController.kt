package eu.adrianistan

import javafx.fxml.FXML
import javafx.collections.FXCollections
import com.azure.messaging.eventhubs.EventProcessorClient
import javafx.scene.control.cell.PropertyValueFactory
import javafx.beans.value.ObservableValue
import com.azure.messaging.eventhubs.EventProcessorClientBuilder
import com.azure.messaging.eventhubs.EventHubClientBuilder
import com.azure.messaging.eventhubs.models.ErrorContext
import com.azure.messaging.eventhubs.models.EventContext
import javafx.application.Platform
import javafx.event.ActionEvent
import javafx.scene.control.*
import javafx.scene.control.cell.MapValueFactory
import kotlinx.serialization.encodeToString
import java.time.Instant
import java.util.function.Consumer
import kotlin.system.exitProcess
import kotlinx.serialization.json.Json

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
    var timestampCol: TableColumn<*, *>? = null

    @FXML
    var properties: TableView<EventHubProperty>? = null

    @FXML
    var keyCol: TableColumn<*, *>? = null

    @FXML
    var valueCol: TableColumn<*, *>? = null

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

    private val format = Json { prettyPrint = true }

    fun initialize() {
        partitionCol!!.cellValueFactory = PropertyValueFactory<EventHubMessage, String>("partition")
        sequenceCol!!.cellValueFactory = PropertyValueFactory<EventHubMessage, Long>("sequence")
        bodyCol!!.cellValueFactory = PropertyValueFactory<EventHubMessage, String>("body")
        timestampCol!!.cellValueFactory = PropertyValueFactory<EventHubMessage, Instant>("timestamp")

        keyCol!!.cellValueFactory = PropertyValueFactory<EventHubProperty, String>("key")
        valueCol!!.cellValueFactory = PropertyValueFactory<EventHubProperty, String>("value")

        table!!.items = data
        table!!.selectionModel.selectionMode = SelectionMode.SINGLE
        table!!.selectionModel.isCellSelectionEnabled = true
        table!!.selectionModel.selectedItemProperty()
            .addListener { obs: ObservableValue<out EventHubMessage>?, oldSelection: EventHubMessage?, newSelection: EventHubMessage? ->
                if (newSelection != null) {
                    val element = Json.parseToJsonElement(newSelection.body)
                    msg!!.text = format.encodeToString(element)
                    properties!!.items = FXCollections.observableList(newSelection.properties)
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

    @FXML
    fun handleDoQuit(event: ActionEvent?) {
        Platform.exit()
        exitProcess(0)
    }

    @FXML
    fun handleDoHelp(event: ActionEvent?) {
        val alert = Alert(Alert.AlertType.INFORMATION)
        alert.title = "About EventHub UI"
        alert.headerText = "EventHub UI 1.0.0"
        alert.contentText = "EventHub UI was made by Adrián Arroyo Calle (https://adrianistan.eu). EventHub is a trademark of Microsoft"
        alert.show()
    }

    val processEvent = Consumer { eventContext: EventContext ->
        System.out.printf("Message received: %s \n", eventContext.eventData.bodyAsString)
        data.add(
            EventHubMessage(
                eventContext.partitionContext.partitionId,
                eventContext.eventData.sequenceNumber,
                eventContext.eventData.bodyAsString,
                eventContext.eventData.enqueuedTime,
                eventContext.eventData.properties.map {
                    EventHubProperty(it.key, it.value.toString())
                }
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