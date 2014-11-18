package es.upm.fi.dia.oeg.morph.base.materializer

import java.io.Writer
import org.apache.log4j.Logger
import com.hp.hpl.jena.ontology.DatatypeProperty
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import com.hp.hpl.jena.rdf.model.ModelFactory

class NTripleMaterializer(model: Model, ntOutputStream: Writer)
        extends MorphBaseMaterializer(model, ntOutputStream) {

    //THIS IS IMPORTANT, SCALA PASSES PARAMETER BY VALUE!
    this.outputStream = ntOutputStream;

    override val logger = Logger.getLogger(this.getClass().getName());

    override def materialize() {
        logger.info("Writing serialization to output stream to " + outputStream)
        this.model.write(this.outputStream, "TURTLE", null)
        this.outputStream.flush();
    }

    override def materializeQuad(subject: RDFNode, predicate: RDFNode, obj: RDFNode, graph: RDFNode) {

        if (graph != null)
            throw new Exception("Named graphs not supported in this version")

        if (subject != null && predicate != null && obj != null) {
            try {
                // Create and add the triple in the Jena model
                val pred = this.model.createProperty(predicate.asResource().getURI())
                val stmt = this.model.createStatement(subject.asResource(), pred, obj)
                this.model.add(stmt)
            } catch {
                case e: Exception => {
                    e.printStackTrace()
                    logger.error("Unable to serialize triple, subject: " + subject);
                }
            }
        } else
            logger.error("Unable to serialize triple, subject: " + subject + ", predicate: " + predicate + ", object: " + obj);
    }
}