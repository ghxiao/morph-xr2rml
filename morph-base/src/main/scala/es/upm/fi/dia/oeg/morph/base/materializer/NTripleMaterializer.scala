package es.upm.fi.dia.oeg.morph.base.materializer

import java.io.Writer

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode

import es.upm.fi.dia.oeg.morph.base.GeneralUtility

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
                val pred = this.model.createProperty(predicate.asResource().getURI())

                var tripleAlreadyExists: Boolean = false

                /*
                 * The following is a bit crappy: in a join with mixed syntax path, the db cannot do the join, as 
                 * a result we run an SQL product to retrieve all possibilities and evaluate them afterwards.
                 * This makes lots of lines with the same results, and we generate several times the same triples.
                 * For triples with simple literal of IRI object, that is fine: Jena does not add twice the same triple.
                 * But for RDF lists and containers, then Jena has no way to know whether this is the same triple or not.
                 * As a result a list or container is created several times in the model.
                 * So in those particular cases, list and containers, we have to check whether the same triple is already
                 * in the model, and if yes then we have to remove the list of container that was passed to the method,
                 * since it exists in the model: a list or container consists of several triples.
                 */

                // Check if the object is an RDF List and if there would already be the same triple in the model
                if (obj.isResource && GeneralUtility.isRdfList(model, obj.asResource)) {
                    // List all triples concerning the subject to see if there would already be the same list
                    val existingObjs = model.listObjectsOfProperty(subject.asResource(), pred)
                    while (!tripleAlreadyExists && existingObjs.hasNext()) {
                        val node = existingObjs.next()
                        if (node.isResource && GeneralUtility.isRdfList(model, node.asResource)) {
                            val same = GeneralUtility.compareRdfList(node.asResource, obj.asResource)
                            // If both lists are the same then we have to remove the new one from the model
                            if (same)
                                GeneralUtility.removeRdfList(model, obj.asResource)
                            tripleAlreadyExists = tripleAlreadyExists || same
                        }
                    }
                }

                // Check if the object is an RDF container and if there would already be the same triple in the model
                if (obj.isResource && GeneralUtility.isRdfContainer(model, obj.asResource)) {
                    // List all triples concerning the subject to see if there would already be the same container
                    val existingObjs = model.listObjectsOfProperty(subject.asResource(), pred)
                    while (!tripleAlreadyExists && existingObjs.hasNext()) {
                        val node = existingObjs.next()
                        if (node.isResource && GeneralUtility.isRdfContainer(model, node.asResource)) {
                            val same = GeneralUtility.compareRdfContainer(node.asResource, obj.asResource)
                            // If both containers are the same, then we have to remove the new one from the model
                            if (same)
                                GeneralUtility.removeRdfContainer(model, obj.asResource)
                            tripleAlreadyExists = tripleAlreadyExists || same
                        }
                    }
                }

                if (!tripleAlreadyExists) {
                    // Create and add the triple into the Jena model
                    val stmt = this.model.createStatement(subject.asResource(), pred, obj)
                    this.model.add(stmt)
                } else {
                    logger.trace("Triple already materialized, ignoring: [" + subject.asResource() + "] [" + pred + "] [" + obj.asResource() + "]")
                }
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
