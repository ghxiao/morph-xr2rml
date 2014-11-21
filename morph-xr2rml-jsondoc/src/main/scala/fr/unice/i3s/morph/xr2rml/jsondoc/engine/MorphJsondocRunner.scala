package fr.unice.i3s.morph.xr2rml.jsondoc.engine

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.AbstractQueryResultTranslator
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import java.io.OutputStream
import java.io.Writer
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import java.io._
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.xR2RML_Constants

class MorphJsondocRunner(
    mappingDocument: R2RMLMappingDocument,
    unfolder: MorphJsondocUnfolder,
    dataTranslator: Option[MorphJsondocDataTranslator],
    queryTranslator: Option[IQueryTranslator],
    resultProcessor: Option[AbstractQueryResultTranslator],
    outputStream: Writer)

        extends MorphBaseRunner(mappingDocument, unfolder, dataTranslator, queryTranslator, resultProcessor, outputStream) {
}

object MorphJsondocRunner {
    val logger = Logger.getLogger(this.getClass());

    def apply(properties: MorphProperties): MorphJsondocRunner = {
        val runner = new MorphJsondocRunnerFactory().createRunner(properties);
        runner.asInstanceOf[MorphJsondocRunner];
    }

    def apply(configDir: String, configFile: String): MorphJsondocRunner = {
        val configurationProperties = MorphProperties(configDir, configFile);
        MorphJsondocRunner(configurationProperties)
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
            val runner = MorphJsondocRunner(configDir, configFile);

            // Start the translation process
            logger.info("Running data translation...");
            runner.run();

            val properties = MorphProperties(configDir, configFile)
            var outputFilepath = properties.outputFilePath.get
            var outputFormat = properties.rdfLanguageForResult

            // Display the result on the std output
            var model = ModelFactory.createDefaultModel().read(FileManager.get().open(outputFilepath), null, "TURTLE")
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