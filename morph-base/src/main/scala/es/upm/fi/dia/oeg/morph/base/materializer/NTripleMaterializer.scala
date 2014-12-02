package es.upm.fi.dia.oeg.morph.base.materializer

import java.io.Writer
import scala.collection.JavaConversions.asScalaIterator
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.rdf.model.Resource
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
                if (obj.isResource && isRdfList(obj.asResource)) {
                    // List all triples concerning the subject to see if there would already be the same list
                    val existingObjs = model.listObjectsOfProperty(subject.asResource(), pred)
                    while (!tripleAlreadyExists && existingObjs.hasNext()) {
                        val node = existingObjs.next()
                        if (node.isResource && isRdfList(node.asResource)) {
                            val same = compareRdfList(node.asResource, obj.asResource)
                            // If both lists are the same then we have to remove the new one from the model
                            if (same)
                                removeRdfList(obj.asResource)
                            tripleAlreadyExists = tripleAlreadyExists || same
                        }
                    }
                }

                // Check if the object is an RDF container and if there would already be the same triple in the model
                if (obj.isResource && isRdfContainer(obj.asResource)) {
                    // List all triples concerning the subject to see if there would already be the same container
                    val existingObjs = model.listObjectsOfProperty(subject.asResource(), pred)
                    while (!tripleAlreadyExists && existingObjs.hasNext()) {
                        val node = existingObjs.next()
                        if (node.isResource && isRdfContainer(node.asResource)) {
                            val same = compareRdfContainer(node.asResource, obj.asResource)
                            // If both containers are the same, then we have to remove the new one from the model
                            if (same)
                                model.removeAll(obj.asResource, null, null)
                            tripleAlreadyExists = tripleAlreadyExists || same
                        }
                    }
                }

                if (!tripleAlreadyExists) {
                    // Create and add the triple into the Jena model
                    val stmt = this.model.createStatement(subject.asResource(), pred, obj)
                    this.model.add(stmt)
                } else {
                    logger.trace("Triple already materialized, ignoring: [" + subject.asResource() + "] [" + pred + "] [" + obj.asResource() +"]" )
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
     * Compare 2 RDF Lists. Return true is they have the same elements.
     * This method does not apply to lists of which members are lists or containers.
     */
    private def compareRdfList(lst1: Resource, lst2: Resource): Boolean = {

        if ((lst1 == null) && (lst2 == null)) return true

        val first1 = model.getProperty(lst1, RDF.first).getObject
        val first2 = model.getProperty(lst2, RDF.first).getObject

        if (first1 != first2) return false

        val rest1 = model.getProperty(lst1, RDF.rest).getObject
        val rest2 = model.getProperty(lst2, RDF.rest).getObject
        if (rest1 == RDF.nil && rest2 == RDF.nil)
            true
        else
            compareRdfList(rest1.asResource, rest2.asResource)
    }

    /**
     * Compare 2 RDF containers (alt, seq, bag). Return true is they have the same elements.
     * This method does not apply to containers of which members are lists or containers.
     */
    private def compareRdfContainer(lst1: Resource, lst2: Resource): Boolean = {
        var i = 0
        var continue: Boolean = true
        var equal: Boolean = true
        while (continue && equal) {
            val item1 = model.getProperty(lst1, RDF.li(i))
            val item2 = model.getProperty(lst2, RDF.li(i))

            if (item1 == null && item2 == null)
                continue = false
            else if ((item1 == null && item2 != null) || (item1 != null && item2 == null))
                equal = false
            else
                equal = (item1.getObject == item2.getObject)
            i = i + 1
        }
        equal
    }

    private def isRdfList(res: Resource): Boolean = {
        val stmtType = model.getProperty(res.asResource(), RDF.`type`)
        (stmtType != null && stmtType.getObject == RDF.List)
    }

    private def isRdfContainer(res: Resource): Boolean = {
        val stmtType = model.getProperty(res.asResource(), RDF.`type`)
        stmtType != null && (
            (stmtType.getObject == RDF.Bag) ||
            (stmtType.getObject == RDF.Alt) ||
            (stmtType.getObject == RDF.Seq))
    }

    /**
     * Recursive removal of all triples concerning an RDF List.
     * This method does not apply to lists of which members are nested lists or containers.
     */
    private def removeRdfList(res: Resource) {
        val rest = model.getProperty(res, RDF.rest)
        if (rest != null)
            removeRdfList(rest.getResource)
        model.removeAll(res, null, null)
    }
}

object NTripleMaterializer {

    def main(args: Array[String]) = {
        val model = ModelFactory.createDefaultModel()

        val a: java.lang.Integer = 3
        val lit1 = model.createTypedLiteral(a)
        val b: java.lang.Integer = 3
        val lit2 = model.createTypedLiteral(b)
        println(lit1.asNode == lit2.asNode)

        // --------------------------------------------------------------------------

        var values = List("a", "b", "d")
        val valuesAsRdfNodes = values.map(value => model.createLiteral(value))
        val node1 = model.createList(valuesAsRdfNodes.toArray[RDFNode])

        var values2 = List("a", "b", "c")
        val valuesAsRdfNodes2 = values2.map(value => model.createLiteral(value))
        val node2 = model.createList(valuesAsRdfNodes2.toArray[RDFNode])

        println("diff lists: " + NTripleMaterializer.compareRdfList(model, node1, node2))
        // --------------------------------------------------------------------------

        values = List("a", "b", "c")
        val list1 = model.createBag()
        for (value <- values) list1.add(value)

        values2 = List("a", "b", "c")
        val list2 = model.createBag()
        for (value <- values2) list2.add(value)

        println("diff bags: " + NTripleMaterializer.compareRdfContainer(model, list1, list2))
    }

    private def compareRdfList(model: Model, lst1: Resource, lst2: Resource): Boolean = {
        if ((lst1 == null) && (lst2 != null)) return false
        if ((lst1 != null) && (lst2 == null)) return false
        if ((lst1 == null) && (lst2 == null)) return true

        val first1 = model.getProperty(lst1, RDF.first).getObject
        val first2 = model.getProperty(lst2, RDF.first).getObject

        if (first1 != first2) return false

        val rest1 = model.getProperty(lst1, RDF.rest).getObject
        val rest2 = model.getProperty(lst2, RDF.rest).getObject
        if (rest1 == RDF.nil && rest2 == RDF.nil)
            true
        else
            NTripleMaterializer.compareRdfList(model, rest1.asResource, rest2.asResource)
    }

    private def compareRdfContainer(model: Model, lst1: Resource, lst2: Resource): Boolean = {
        var i = 1
        var continue: Boolean = true
        var equal: Boolean = true
        while (continue && equal) {
            val item1 = model.getProperty(lst1, RDF.li(i))
            val item2 = model.getProperty(lst2, RDF.li(i))

            if (item1 == null && item2 == null)
                continue = false
            else if ((item1 == null && item2 != null) || (item1 != null && item2 == null))
                equal = false
            else
                equal = (item1.getObject == item2.getObject)
            i = i + 1
        }
        equal
    }
}
