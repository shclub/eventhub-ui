package eu.adrianistan

import javafx.application.Application
import javafx.fxml.FXMLLoader
import javafx.scene.Parent
import javafx.scene.Scene
import javafx.stage.Stage
import java.io.IOException
import kotlin.jvm.JvmStatic

/**
 * JavaFX App
 */
class App : Application() {
    override fun start(stage: Stage) {
        try {
            val root = FXMLLoader.load<Parent>(javaClass.getResource("/main.fxml"))
            val scene = Scene(root)
            stage.title = "EventHub UI"
            stage.scene = scene
            stage.show()
        } catch (exp: IOException) {
            exp.printStackTrace()
        }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            launch(App::class.java)
        }
    }
}