package eu.adrianistan;

import java.util.function.Consumer;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.TableView;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.cell.PropertyValueFactory;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;

public class MainController {
    @FXML
    public TableView<EventHubMessage> table;

    @FXML
    public TableColumn partitionCol;

    @FXML
    public TableColumn sequenceCol;

    @FXML
    public TableColumn bodyCol;

    @FXML
    public TextField connectionString;

    @FXML
    public TextField hubName;

    @FXML
    public Button connect;

    @FXML
    public TextArea msg;

    private final ObservableList<EventHubMessage> data = FXCollections.observableArrayList();

    private EventProcessorClient eventProcessorClient;

    public MainController(){
        this.eventProcessorClient = null;
    }

    public void initialize(){
        partitionCol.setCellValueFactory(
            new PropertyValueFactory<EventHubMessage, String>("partition")
        );
        sequenceCol.setCellValueFactory(
            new PropertyValueFactory<EventHubMessage, Long>("sequence")
        );
        bodyCol.setCellValueFactory(
            new PropertyValueFactory<EventHubMessage, String>("body")
        );
        table.setItems(data);

        table.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
        table.getSelectionModel().setCellSelectionEnabled(true);
        table.getSelectionModel().selectedItemProperty().addListener((obs, oldSelection, newSelection) -> {
            if(newSelection != null){
                msg.setText(newSelection.getBody());
            }
        });
    }

    @FXML
    public void handleDoConnect(ActionEvent event){
        if(eventProcessorClient == null){
            EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
                .connectionString(connectionString.getText(), hubName.getText())
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .processEvent(this.processEvent)
                .processError(this.processError)
                .checkpointStore(new NullCheckpointStore());

            eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();
            eventProcessorClient.start();

            connect.setText("Close connection");
        } else {
            eventProcessorClient.stop();
            eventProcessorClient = null;
            connect.setText("Connect");
        }
    }

    public final Consumer<EventContext> processEvent = eventContext -> {
        System.out.printf("Message received: %s \n", eventContext.getEventData().getBodyAsString());
        data.add(new EventHubMessage(
            eventContext.getPartitionContext().getPartitionId(),
            eventContext.getEventData().getSequenceNumber(),
            eventContext.getEventData().getBodyAsString()
        ));
    };

    public final Consumer<ErrorContext> processError = errorContext -> {
        System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
            errorContext.getPartitionContext().getPartitionId(),
            errorContext.getThrowable());
    };    
}