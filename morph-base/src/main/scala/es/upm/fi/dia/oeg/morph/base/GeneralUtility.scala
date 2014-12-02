package es.upm.fi.dia.oeg.morph.base

import java.net.URL

import org.apache.log4j.Logger

import com.hp.hpl.jena.rdf.model.RDFNode

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
            //result = result.replaceAll("\\%", "%25");//put this first
            result = result.replaceAll("<", "%3C");
            result = result.replaceAll(">", "%3E");
            //			result = result.replaceAll("#", "%23");
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

}
