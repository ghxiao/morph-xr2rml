package es.upm.fi.dia.oeg.morph.base.materializer

import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Resource
import java.io.FileOutputStream
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Model
import java.io.OutputStream
import java.io.Writer

class RDFXMLMaterializer(model: Model, rdfxmlOutputStream: Writer)
  extends MorphBaseMaterializer(model, rdfxmlOutputStream) {
  //THIS IS IMPORTANT, SCALA PASSES PARAMETER BY VALUE!
  this.outputStream = rdfxmlOutputStream;

  override val logger = Logger.getLogger(this.getClass().getName());
  var outputFileName: String = null;

  override def materialize() = {
    try {
      if (model != null) {
				logger.debug("Size of model = " + model.size());
        logger.info("Writing model to " + outputFileName + " ......");
        val startWritingModel = System.currentTimeMillis();
        //val fos = new FileOutputStream(outputFileName);
        model.write(this.outputStream, this.rdfLanguage);
        this.outputStream.flush();
        //				this.outputStream.close();
        val endWritingModel = System.currentTimeMillis();
        val durationWritingModel = (endWritingModel - startWritingModel) / 1000;
        logger.info("Writing model time was " + (durationWritingModel) + " s.");
      }
    } catch {
      case e: Exception => {
        logger.error("Error materializing: " + e.getMessage);
        throw e;
      }
    }
  }

  override def materializeQuad(subject: RDFNode, predicate: RDFNode,
    obj: RDFNode, graph: RDFNode) {
    // TODO Auto-generated method stub
  }

}