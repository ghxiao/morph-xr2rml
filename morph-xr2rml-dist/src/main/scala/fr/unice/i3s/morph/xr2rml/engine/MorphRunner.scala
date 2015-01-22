package fr.unice.i3s.morph.xr2rml.engine

import java.io.FileWriter

import org.apache.commons.cli.BasicParser
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunnerFactory

object MorphRunner {
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

            // Create the runner, parse the mapping document, create the unfolder, data materializer, data translator etc.
            val properties = MorphProperties(configDir, configFile)

            val runnerFactInstance = Class.forName(properties.runnerFactoryClassName).newInstance()
            val runnerFact = runnerFactInstance.asInstanceOf[MorphBaseRunnerFactory]
            val runner = runnerFact.createRunner(properties);

            // Start the translation process
            logger.info("Running data translation...")
            runner.run()

            // Reload the resulting file and display it on the std output
            var outputFilepath = properties.outputFilePath.get
            var model = ModelFactory.createDefaultModel().read(FileManager.get().open(outputFilepath), null, Constants.DEFAULT_OUTPUT_FORMAT)
            model.write(System.out, Constants.DEFAULT_OUTPUT_FORMAT, null)

            var outputFormat = properties.rdfLanguageForResult
            if (outputFormat != Constants.DEFAULT_OUTPUT_FORMAT) {
                // Save the result in the output file again but with the requested format (in case it is different)
                logger.info("Saving output to format " + outputFormat + "...");
                MorphBaseRunner.erasefile(outputFilepath)
                model.write(new FileWriter(outputFilepath), outputFormat)
            }
        } catch {
            case e: com.hp.hpl.jena.n3.turtle.TurtleParseException => {
                logger.fatal("Invalid xR2RML document, parsing error: " + e.getMessage)
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