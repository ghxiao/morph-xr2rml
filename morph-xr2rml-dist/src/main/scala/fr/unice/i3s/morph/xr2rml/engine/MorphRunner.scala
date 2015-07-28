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
            if (cmd.hasOption("d"))
                configDir = cmd.getOptionValue("d")
            if (cmd.hasOption("f"))
                configFile = cmd.getOptionValue("f")

            logger.info("properties Directory = " + configDir)
            logger.info("properties File      = " + configFile)

            val properties = MorphProperties(configDir, configFile)

            // Create the runner factory based on the class name given in configuration file
            val runnerFact = Class.forName(properties.runnerFactoryClassName).newInstance().asInstanceOf[MorphBaseRunnerFactory]

            // Create the runner, parse the mapping document, create the unfolder, data materializer, data translator etc.
            val runner = runnerFact.createRunner(properties);

            // Start the translation process
            logger.info("Running data translation...")
            runner.run()

            var outputFilepath = properties.outputFilePath.get
            var outputFormat = properties.rdfLanguageForResult

            if (properties.outputDisplay || !outputFormat.equals(Constants.DEFAULT_OUTPUT_FORMAT)) {

                // Reload the resulting file
                var model = ModelFactory.createDefaultModel().read(FileManager.get().open(outputFilepath), null, Constants.DEFAULT_OUTPUT_FORMAT)
                if (properties.outputDisplay) {
                    // Display the result on the std output
                    model.write(System.out, Constants.DEFAULT_OUTPUT_FORMAT, null)
                }

                if (!outputFormat.equals(Constants.DEFAULT_OUTPUT_FORMAT)) {
                    // Save the result in the output file again but with the requested format (in case it is different)
                    logger.info("Saving output to format " + outputFormat + "...");
                    // MorphBaseRunner.erasefile(outputFilepath)
                    model.write(new PrintWriter(properties.outputFilePath.get + "_rewritten.rdf", "UTF-8"), outputFormat)
                }
            }

            logger.info("Treatment completed, exiting.");

        } catch {
            case e: com.hp.hpl.jena.n3.turtle.TurtleParseException => {
                logger.fatal("Invalid xR2RML document, parsing error: " + e.getMessage)
                e.printStackTrace()
                System.exit(-1)
            }
            case e: Exception => {
                logger.fatal("An unexpected exception occured: " + e.getMessage())
                e.printStackTrace()
                System.exit(-1)
            }
        }
    }
}