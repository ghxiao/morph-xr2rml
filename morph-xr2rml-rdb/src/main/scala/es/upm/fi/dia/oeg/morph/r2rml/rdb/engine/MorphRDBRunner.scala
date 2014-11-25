package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.io.BufferedWriter
import java.io.FileWriter
import java.io.IOException
import java.io.PrintWriter
import java.io.Writer

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager

import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument

class MorphRDBRunner(
    mappingDocument: R2RMLMappingDocument,
    unfolder: MorphRDBUnfolder,
    dataTranslator: Option[MorphRDBDataTranslator],
    queryTranslator: Option[IQueryTranslator],
    resultProcessor: Option[AbstractQueryResultTranslator],
    outputStream: Writer)
    
        extends MorphBaseRunner(mappingDocument, unfolder, dataTranslator, queryTranslator, resultProcessor, outputStream) {
}

object MorphRDBRunner {
    val logger = Logger.getLogger(this.getClass());

    def apply(properties: MorphProperties): MorphRDBRunner = {
        val runner = new MorphRDBRunnerFactory().createRunner(properties);
        runner.asInstanceOf[MorphRDBRunner];
    }

    def apply(configDir: String, configFile: String): MorphRDBRunner = {
        val configurationProperties = MorphProperties(configDir, configFile);
        MorphRDBRunner(configurationProperties)
    }

    def erasefile(file: String) {
        try {
            var out = new PrintWriter(new BufferedWriter(new FileWriter(file, false)))
            out.print("")
            out.flush()
            out.close()
        } catch {
            case e: IOException => { logger.info("Error with file " + file); }
        }
    }

    def main(args: Array[String]) {
        try {
            val configDir = "examples";
            val configFile = "morph.properties";

            logger.info("properties Directory = " + configDir);
            logger.info("properties File      = " + configFile);
            
            // Create the runner, parse the mapping document, create the unfolder, data materializer, data translator etc.
            val runner = MorphRDBRunner(configDir, configFile);
            
            // Start the translation process
            logger.info("Running data translation...");
            runner.run();

            val properties = MorphProperties(configDir, configFile)
            var outputFilepath = properties.outputFilePath.get
            var outputFormat = properties.rdfLanguageForResult

            var model = ModelFactory.createDefaultModel().read(FileManager.get().open(outputFilepath), null, "TURTLE")

            // Display the result on the std output
            model.write(System.out, outputFormat, null)

            // Save the result in the output file again but with the requested format (in case it is different)
            erasefile(outputFilepath)
            model.write(new FileWriter(outputFilepath), outputFormat)
        } catch {
            case e: Exception => {
                logger.error("Exception occured: " + e.getMessage());
                throw e;
            }
        }
    }
}