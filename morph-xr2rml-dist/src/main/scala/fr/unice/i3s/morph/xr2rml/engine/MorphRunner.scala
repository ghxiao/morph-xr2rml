package fr.unice.i3s.morph.xr2rml.engine

import java.io.FileWriter
import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import java.io.FileInputStream
import scala.io.Source

/**
 * MorphRunner is the main entry point of the Morph-xR2RML application.
 * It expects two input parameters: the configuration directory (option --configDir) and the
 * configuration file name (option --configFile) that must be located in the configuration directory.
 * The configuration file lists, among others, the name of the mapping file and the output file.
 */
object MorphRunner {

    // Log4j init
    val log4jfile = this.getClass().getClassLoader().getResource("xr2rml-log4j.properties")
    println("Loading log4j configuration: " + log4jfile)
    PropertyConfigurator.configure(log4jfile)
    val logger = Logger.getLogger(this.getClass());

    def main(args: Array[String]) {
        try {
            // Default config dir and file
            var configDir = "example_mysql"
            var configFile = "morph.properties"

            // Parse the command line arguments
            val options: org.apache.commons.cli.Options = new org.apache.commons.cli.Options()
            options.addOption("d", "configDir", true, "Configuration directory")
            options.addOption("f", "configFile", true, "Configuration file name. Must be located within the configuration directory")

            val parser: CommandLineParser = new BasicParser()
            val cmd: CommandLine = parser.parse(options, args)
            if (cmd.hasOption("d")) configDir = cmd.getOptionValue("d")
            if (cmd.hasOption("f")) configFile = cmd.getOptionValue("f")
            logger.info("properties Directory = " + configDir)
            logger.info("properties File      = " + configFile)

            // Create the runner factory based on the class name given in configuration file
            val properties = MorphProperties(configDir, configFile)
            val runnerFact = MorphBaseRunnerFactory.createFactory(properties)

            // Create the runner and start the translation process
            val runner = runnerFact.createRunner
            logger.info("Running data translation...")
            runner.run()

            if (properties.outputDisplay) {
                if (properties.outputFilePath.isDefined) {
                    logger.info("Query result:")
                    Source.fromFile(properties.outputFilePath.get).foreach { print }
                }
            }

            logger.info("Treatment completed, exiting.");

        } catch {
            case e: com.hp.hpl.jena.n3.turtle.TurtleParseException => {
                logger.fatal("Invalid xR2RML document, parsing error: " + e.getMessage)
                e.printStackTrace()
                System.exit(-1)
            }
            case e: MorphException => {
                logger.fatal("An error has occured: " + e.getMessage())
                e.printStackTrace()
                System.exit(-1)
            }
            case e: Exception => {
                logger.fatal("An unexpected error occured: " + e.getMessage())
                e.printStackTrace()
                System.exit(-1)
            }
        }
    }
}