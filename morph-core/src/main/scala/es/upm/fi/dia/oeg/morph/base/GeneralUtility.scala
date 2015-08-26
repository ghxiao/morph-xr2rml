package es.upm.fi.dia.oeg.morph.base

import java.net.URL
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.vocabulary.RDF

object GeneralUtility {
    val logger = Logger.getLogger("GeneralUtility");

    def encodeLiteral(originalLiteral: String): String = {
        var result = originalLiteral;
        try {
            if (result != null) {
                result = result.replaceAll("\\\\", "/");
                result = result.replaceAll("\"", "%22");
                result = result.replaceAll("\\\\n", " ");
                result = result.replaceAll("\\\\r", " ");
                result = result.replaceAll("\\\\ ", " ");
                result = result.replaceAll("_{2,}+", "_");
                result = result.replaceAll("\n", "");
                result = result.replaceAll("\r", "");
                result = result.replace("\\ ", "/");
            }
        } catch {
            case e: Exception => {
                logger.error("Error encoding literal for literal = " + originalLiteral + " because of " + e.getMessage());
            }
        }
        result;
    }

    def encodeURI(originalURI: String, mapURIEncodingChars: Map[String, String], uriTransformationOperations: List[String]): String = {
        val resultAux = originalURI.trim();
        var result = resultAux;
        try {
            if (mapURIEncodingChars != null) {
                mapURIEncodingChars.foreach {
                    case (key, value) => result = result.replaceAll(key, value)
                }
            }
        } catch {
            case e: Exception => {
                logger.error("Error encoding uri for uri = " + originalURI + " because of " + e.getMessage());
                resultAux
            }
        }
        result;
    }

    def isNetResource(resourceAddress: String): Boolean = {
        val result = try {
            val url = new URL(resourceAddress);
            val conn = url.openConnection();
            conn.connect();
            true;
        } catch {
            case e: Exception => { false }
        }
        result;
    }

    def encodeUnsafeChars(originalValue: String): String = {
        var result = originalValue;
        if (result != null) {
            // result = result.replaceAll("\\%", "%25"); //put this first
            // result = result.replaceAll("#", "%23");
            result = result.replaceAll("<", "%3C");
            result = result.replaceAll(">", "%3E");
            result = result.replaceAll("\\{", "%7B");
            result = result.replaceAll("\\}", "%7D");
            result = result.replaceAll("\\|", "%7C");
            result = result.replaceAll("\\\\", "%5C");
            result = result.replaceAll("\\^", "%5E");
            result = result.replaceAll("~", "%7E");
            result = result.replaceAll("\\[", "%5B");
            result = result.replaceAll("\\]", "%5D");
            result = result.replaceAll("`", "%60");
            result = result.replaceAll(" ", "%20");
            result = result.replaceAll("\"", "%22");
        }
        result;
    }

    def encodeReservedChars(originalValue: String): String = {
        var result = originalValue;
        if (result != null) {
            result = result.replaceAll("\\$", "%24");
            result = result.replaceAll("&", "%26");
            result = result.replaceAll("\\+", "%2B");
            result = result.replaceAll(",", "%2C");
            result = result.replaceAll(";", "%3B");
            result = result.replaceAll("=", "%3D");
            result = result.replaceAll("\\?", "%3F");
            result = result.replaceAll("@", "%40");
            result = result.replaceAll("/", "%2F");
            result = result.replaceAll(":", "%3A");
        }
        result;
    }

    /**
     * Recursive method to compute the intersection of multiple sets of RDFNode
     *
     * @return the intersection, possibly empty
     */
    def intersectMultipleSets(sets: Set[List[RDFNode]]): List[RDFNode] = {
        if (sets.size == 0)
            List()
        else if (sets.size > 1)
            sets.head.intersect(intersectMultipleSets(sets.tail))
        else sets.head
    }

    /**
     * Compare 2 RDF Lists. Return true is they have the same elements.
     * This method does not apply to lists of which members are lists or containers.
     */
    def compareRdfList(lst1: Resource, lst2: Resource): Boolean = {

        if ((lst1 == null) && (lst2 != null)) return false
        if ((lst1 != null) && (lst2 == null)) return false
        if ((lst1 == null) && (lst2 == null)) return true

        val first1 = lst1.getProperty(RDF.first).getObject
        val first2 = lst2.getProperty(RDF.first).getObject

        if (first1 != first2) return false

        val rest1 = lst1.getProperty(RDF.rest).getObject
        val rest2 = lst2.getProperty(RDF.rest).getObject

        if ((rest1 == RDF.nil && rest2 != RDF.nil) || (rest1 != RDF.nil && rest2 == RDF.nil))
            false
        else if (rest1 == RDF.nil && rest2 == RDF.nil)
            true
        else
            GeneralUtility.compareRdfList(rest1.asResource, rest2.asResource)
    }

    /**
     * Compare 2 RDF containers (alt, seq, bag). Return true is they have the same elements.
     * This method does not apply to containers of which members are lists or containers.
     */
    def compareRdfContainer(lst1: Resource, lst2: Resource): Boolean = {
        var i = 1
        var continue: Boolean = true
        var equal: Boolean = true
        while (continue && equal) {
            val item1 = lst1.getProperty(RDF.li(i))
            val item2 = lst2.getProperty(RDF.li(i))
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

    /**
     * Recursive removal of all triples concerning an RDF List.
     * This method does not apply to lists of which members are nested lists or containers.
     */
    def removeRdfList(model: Model, res: Resource) {
        val rest = model.getProperty(res, RDF.rest)
        if (rest != null)
            GeneralUtility.removeRdfList(model, rest.getResource)
        model.removeAll(res, null, null)
    }

    /**
     * Removal of all triples concerning an RDF bag, seq or alt.
     * This method does not apply to container of which members are nested lists or containers.
     */
    def removeRdfContainer(model: Model, res: Resource) {
        model.removeAll(res, null, null)
    }

    def isRdfList(model: Model, res: Resource): Boolean = {
        val stmtType = model.getProperty(res.asResource(), RDF.`type`)
        (stmtType != null && stmtType.getObject == RDF.List)
    }

    def isRdfContainer(model: Model, res: Resource): Boolean = {
        val stmtType = model.getProperty(res.asResource(), RDF.`type`)
        stmtType != null && (
            (stmtType.getObject == RDF.Bag) ||
            (stmtType.getObject == RDF.Alt) ||
            (stmtType.getObject == RDF.Seq))
    }
}

