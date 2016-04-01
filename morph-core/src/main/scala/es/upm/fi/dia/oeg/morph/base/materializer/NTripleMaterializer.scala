package es.upm.fi.dia.oeg.morph.base.materializer

import java.io.Writer

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode

import es.upm.fi.dia.oeg.morph.base.GeneralUtility

class NTripleMaterializer(model: Model, outputStream: Writer)
        extends MorphBaseMaterializer(model, outputStream) {

    override val logger = Logger.getLogger(this.getClass().getName())

    /**
     * Serialize the current model into the output file
     */
    override def materialize() {
        materialize(this.model)
    }

    /**
     * Utility method to serialize any model into the output file
     */
    override def materialize(model: Model) {
        logger.info("Model size (in triples): " + model.size())
        logger.info("Writing serialization to output stream " + outputStream)
        model.write(this.outputStream, "TURTLE", null)
        this.outputStream.flush()
        this.outputStream.close()
    }

    override def materializeQuad(subject: RDFNode, predicate: RDFNode, obj: RDFNode, graph: RDFNode) {

        if (graph != null)
            throw new Exception("Named graphs not supported in this version")

        if (subject != null && predicate != null && obj != null) {
            try {
                val pred = this.model.createProperty(predicate.asResource().getURI())

                var tripleAlreadyExists: Boolean = false

                /* Case RDB:
                 * The following is a bit crappy: in a join with mixed syntax path, the join cannot be done in SQL,
                 * as a result we run an SQL cartesian product to retrieve all possibilities and evaluate them afterwards.
                 * This makes lots of lines with the same results, and we generate several times the same triples.
                 * For triples with simple literal of IRI object, that is fine: Jena does not add twice the same triple.
                 * But for RDF lists and containers, Jena has no way to know whether this is the same triple or not.
                 * As a result a list or container is created several times in the model.
                 * So, in those particular cases, list and containers, we have to check whether the same triple is already
                 * in the model. If yes,
                 * - first we do not add a triple.
                 * - but that not enough. The collection/container passed to the method (parameter obj),
                 *   exists in the model since it consists of several triples. 
                 *   Therefore, we have to remove this collection/container from the model.
                 */

                // Check if the object is an RDF List and if there would already be the same triple in the model
                if (obj.isResource && GeneralUtility.isRdfList(model, obj.asResource)) {

                    // List all triples concerning the same subject and predicate to see if there would already be the same list
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

    /**
     * Materialize RDF triple in target graphs
     *
     * @return number of triples generated
     */
    override def materializeQuads(
        subjects: List[RDFNode],
        predicates: List[RDFNode],
        objects: List[RDFNode],
        refObjects: List[RDFNode],
        graphs: List[RDFNode]): Integer = {

        var nbTriples = 0
        predicates.foreach(pred => {
            subjects.foreach(sub => {
                objects.foreach(obj => {
                    if (graphs.isEmpty) {
                        this.materializeQuad(sub, pred, obj, null)
                        nbTriples += 1
                        if (logger.isDebugEnabled()) logger.debug("Materialized triple: [" + sub + "] [" + pred + "] [" + obj + "]")
                    } else {
                        graphs.foreach(graph => {
                            this.materializeQuad(sub, pred, obj, graph)
                            nbTriples += 1
                            if (logger.isDebugEnabled()) logger.debug("Materialized triple: graph[" + graph + "], [" + sub + "] [" + pred + "] [" + obj + "]")
                        })
                    }
                })
                refObjects.foreach(obj => {
                    if (obj != null) {
                        if (graphs.isEmpty) {
                            this.materializeQuad(sub, pred, obj, null)
                            nbTriples += 1
                            if (logger.isDebugEnabled()) logger.debug("Materialized triple: [" + sub + "] [" + pred + "] [" + obj + "]")
                        } else {
                            graphs.foreach(graph => {
                                this.materializeQuad(sub, pred, obj, graph)
                                nbTriples += 1
                                if (logger.isDebugEnabled()) logger.debug("Materialized triple: graph[" + graph + "], [" + sub + "] [" + pred + "] [" + obj + "]")
                            })
                        }
                    }
                })
            })
        })
        nbTriples
    }

}
