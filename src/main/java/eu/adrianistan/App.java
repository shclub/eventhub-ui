package eu.adrianistan;

import java.io.IOException;
import java.net.URL;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;

/**
 * JavaFX App
 */
public class App extends Application {

    @Override
    public void start(Stage stage) {
        try{
            Parent root = FXMLLoader.load(getClass().getResource("/main.fxml"));
            var scene = new Scene(root);
            stage.setTitle("EventHub UI");
            stage.setScene(scene);
            stage.show();
        } catch (IOException exp) {
            exp.printStackTrace();
        }
    }

    public static void main(String[] args) {
        launch();
    }

}