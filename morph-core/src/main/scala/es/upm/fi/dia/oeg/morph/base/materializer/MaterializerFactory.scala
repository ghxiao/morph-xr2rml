package es.upm.fi.dia.oeg.morph.base.materializer

import es.upm.fi.dia.oeg.morph.base.Constants
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.Writer
import org.apache.log4j.Logger

object MaterializerFactory {
    val logger = Logger.getLogger(this.getClass());

    def create(outputStream: Writer, jenaMode: String): MorphBaseMaterializer = {

        logger.info("Creating MorphBaseMaterializer. Jena mode: " + jenaMode + ", Output stream: " + outputStream)

        val model = MorphBaseMaterializer.createJenaModel(jenaMode);

        val materializer = new NTripleMaterializer(model, outputStream);
        materializer
    }
}