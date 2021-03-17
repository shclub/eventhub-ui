module app {
    requires javafx.controls;
    requires javafx.fxml;
    requires transitive kotlin.stdlib;
    requires com.azure.messaging.eventhubs;
    requires com.google.gson;

    opens eu.adrianistan to javafx.fxml;
    exports eu.adrianistan;
}