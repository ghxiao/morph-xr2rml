package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField

class JsonPathToMongoTranslator {

}

object JsonPathToMongoTranslator {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Regex for JSONPath expressions containing a concatenation of single field names (with tailing-dot or in array notation)
     * and array indexes, i.e. one of: .p, ["p"] or [i]. E.g. '.p.q.r', '.p[10]["r"]'
     * '*' is NOT allowed. Field alternatives (["q","r"]) and array index alternatives ([1,2]) are NOT allowed.
     */
    final val JSONPATH_PATH_NS = """^(\.\p{Alnum}+|\["\p{Alnum}+"\]|\[\p{Digit}+\])+""".r

    /**
     * Regex for JSONPath expressions containing a concatenation of single field names (with tailing-dot or in array notation)
     * and array indexes, i.e. one of: .p, ["p"] or [i]. E.g. '.p.q.r', '.p[10]["r"]'
     * '*' is NOT allowed. Field alternatives (["q","r"]) and array index alternatives ([1,2]) are NOT allowed.
     */
    final val JSONPATH_PATH = """^(\.\p{Alnum}+|\["\p{Alnum}+"\]|\[\p{Digit}+\]|\[\*\]|\.\*)+""".r

    /**
     *  Regex for a JSONPath field name with heading dot: .p
     *  The full match will be .p but the first capturing group will only p
     */
    final val JSONPATH_FIELD_NAME_DOTTED = """^\.(\p{Alnum}+)""".r

    /**
     *  Regex for a JSONPath field name in array notation: ["p"]
     *  The full match will be ["p"] but the first capturing group will only p
     */
    final val JSONPATH_FIELD_NAME_ARRAY_NOTATION = """^\["(\p{Alnum}+)"\]""".r

    /**
     *  Regex for a JSONPath field name in double-quoted in an alternatives: "q","r"
     *  The full match will be like "p" (including double-quotes), the first capturing group will only the p
     *  (without the double-quotes)
     */
    final val JSONPATH_FIELD_NAME_QUOTED = """"(\p{Alnum}+)"""".r

    /**
     * Regex for JSONPath field alternatives: ["q","r", ...]
     * The "?:" and the beginning of the group just indicates to not show it as part of the captured groups.
     */
    final val JSONPATH_FIELD_ALTERNATIVE = """^(\["\p{Alnum}+"(?:,"\p{Alnum}+")+\])""".r

    /**
     *  Regex for a JSONPath array index in array notation: [5]
     *  The full match will be [5] but the first capturing group will only 5
     */
    final val JSONPATH_ARRAY_IDX_ARRAY_NOTATION = """^\[(\p{Digit}+)\]""".r

    /**
     * Regex for JSONPath array index: basically an integer.
     */
    final val JSONPATH_ARRAY_IDX = """\p{Digit}+""".r

    /**
     * Regex for JSONPath array index alternatives: [1, 3, ...]
     * The "?:" and the beginning of the group just indicates to not show it as part of the captured groups,
     * instead only the parent group is captured.
     */
    final val JSONPATH_ARRAY_IDX_ALTERNATIVE = """^(\[\p{Digit}+(?:,\p{Digit}+)+\])""".r

    /**
     * Regex for rule r2 matching JSONPath expressions like: <JPpath_ns>["p","q",...]<JPexpr>
     */
    final val R2_REGEXP = ("^(" + JSONPATH_PATH_NS + ")(" + JSONPATH_FIELD_ALTERNATIVE + ")(.*)").r

    def trans(jpPath: String, cond: MongoQueryNodeCond): MongoQueryNode = {
        if (logger.isTraceEnabled()) logger.trace("trans(" + jpPath + ", " + cond.toQueryString + ")")

        // Clean string: remove spaces, use only double-quotes
        val path = jpPath.trim().replace("'", "\"").replaceAll("""\p{Space}""", "")

        // ------ Rule 0
        if (path.isEmpty())
            return cond

        // ------ Rule 1
        if (path.charAt(0) == '$')
            return trans(path.substring(1), cond)

        // ------ Rule 2: Field alternative or array index alternative
        {
            // First, try to match <JPpath_ns> at the beginning of the path
            val match0_JPpath_ns = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(path).toList

            if (!match0_JPpath_ns.isEmpty) {
                // Match 0 is a <JPpath_ns>
                val match0 = match0_JPpath_ns(0).group(0)
                val after_match0 = match0_JPpath_ns(0).after(0)

                // ------ Rule 2a:
                // trans(<JPpath_ns>["p","q",...]<JPexpr>, <cond>) :-
                //        OR( trans(<JPpath_ns>.p<JPexpr>, <cond>), trans(<JPpath_ns>.q<JPexpr>, <cond>), ... )
                var result = translateFieldAlternative(match0, after_match0.toString(), cond)
                if (result.isDefined) return result.get

                // ------ Rule 2b:
                // trans(<JPpath_ns>[1,3,...]<JPexpr>, <cond>) :-
                //        OR( trans(<JPpath_ns>.1<JPexpr>, <cond>), trans(<JPpath_ns>.3<JPexpr>, <cond>), ... )
                result = translateArrayIndexAlternative(match0, after_match0.toString(), cond)
                if (result.isDefined) return result.get
            }
        }

        // ------ Rule 3: Heading field alternative or array index alternative
        {
            // ------ Rule 3a:
            // trans(["p","q",...]<JPexpr>, <cond>) :-
            //        OR( trans(.p<JPexpr>, <cond>), trans(.q<JPexpr>, <cond>), ... )
            var result = translateFieldAlternative("", path, cond)
            if (result.isDefined) return result.get

            // ------ Rule 3b:
            // trans([1,3,...]<JPexpr>, <cond>) :-
            //        OR( trans(.1<JPexpr>, <cond>), 
            //            trans(.3<JPexpr>, <cond>), ... )
            result = translateArrayIndexAlternative("", path, cond)
            if (result.isDefined) return result.get
        }

        // ------ Rule 8: Heading field name or array index
        {
            var fieldMatch: String = ""
            var afterMatch: String = ""

            // Try to match .p
            var matched = JsonPathToMongoTranslator.JSONPATH_FIELD_NAME_DOTTED.findAllMatchIn(path).toList
            if (!matched.isEmpty) {
                fieldMatch = matched(0).group(1)
                afterMatch = matched(0).after(0).toString
            }

            // Try to match ["p"]
            matched = JsonPathToMongoTranslator.JSONPATH_FIELD_NAME_ARRAY_NOTATION.findAllMatchIn(path).toList
            if (!matched.isEmpty) {
                fieldMatch = matched(0).group(1)
                afterMatch = matched(0).after(0).toString
            }

            // Try to match [5]
            matched = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX_ARRAY_NOTATION.findAllMatchIn(path).toList
            if (!matched.isEmpty) {
                fieldMatch = matched(0).group(1)
                afterMatch = matched(0).after(0).toString
            }

            if (!fieldMatch.isEmpty) {
                if (logger.isTraceEnabled()) logger.trace("Rule 8, matched: " + fieldMatch);
                return new MongoQueryNodeField(fieldMatch, trans(afterMatch, cond))
            }
        }

        null
    }

    /**
     * Translates a field alternative (such as ["p","q",...]<JPexpr>) into an OR query operator:
     * OR( trans(.p<JPexpr>, <cond>), trans(.p<JPexpr>, <cond>), ... ).
     * It is used in the processing of rules 2a and 3a.
     *
     * @param prePath part of the JSONPath that was before altPath. It may be empty or null.
     * @param altPath a JSONPath that should start with a field alternative
     * @param cond the global condition (not null of equality)
     * @result a MongoQueryNode if altPath was a field alternative, none otherwise.
     */
    private def translateFieldAlternative(prePath: String, altPath: String, cond: MongoQueryNodeCond): Option[MongoQueryNode] = {
        val pre =
            if (prePath == null || prePath.isEmpty())
                ""
            else prePath

        // Try to match a field alternative ["p","q",...]
        val match0_FieldAlt = JsonPathToMongoTranslator.JSONPATH_FIELD_ALTERNATIVE.findAllMatchIn(altPath).toList
        if (match0_FieldAlt.size > 1)
            throw new MorphException("Invalid match: retrieved more than 1 match of [\"p\",\"q\",...] in " + altPath)

        if (!match0_FieldAlt.isEmpty) {
            val match0 = match0_FieldAlt(0).group(0) // Match 0 is a field alternative ["p","q",...]
            val after_match0 = match0_FieldAlt(0).after(0) // The rest (after match0) may be empty
            if (logger.isTraceEnabled()) logger.trace("match: " + match0 + ", after_match: " + after_match0)

            // Find each individual field name in the alternative
            val match1_FieldAltNames = JsonPathToMongoTranslator.JSONPATH_FIELD_NAME_QUOTED.findAllMatchIn(match0).toList

            // Build the members of the OR query operator: .p<JPexpr>, .q<JPexpr>, etc.
            val orMembers = match1_FieldAltNames.map({ g => pre + "." + g.group(1) + after_match0 })
            if (logger.isDebugEnabled()) logger.debug("OR members: " + orMembers)

            // Run the translation of each OR member
            Some(new MongoQueryNodeOr(orMembers.map(om => trans(om, cond))))
        } else
            None
    }

    /**
     * Translates an array index alternative (such as [1,3,...]<JPexpr>) into an OR query operator:
     * OR( trans(.1<JPexpr>, <cond>), trans(.3<JPexpr>, <cond>), ... ).
     * It is used in the processing of rules 2b and 3b.
     *
     * @param prePath part of the JSONPath that was before altPath. It may be empty or null.
     * @param altPath a JSONPath that should start with an array index alternative
     * @param cond the global condition (not null of equality)
     * @result a MongoQueryNode if altPath was an array index alternative, none otherwise.
     */
    private def translateArrayIndexAlternative(prePath: String, altPath: String, cond: MongoQueryNodeCond): Option[MongoQueryNode] = {
        val pre =
            if (prePath == null || prePath.isEmpty())
                ""
            else prePath

        // Try to match a field alternative [1,3,...]
        val match0_ArrayIdxAlt = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX_ALTERNATIVE.findAllMatchIn(altPath).toList
        if (match0_ArrayIdxAlt.size > 1)
            throw new MorphException("Invalid match: retrieved more than 1 match of \"[1,3,...]\" in " + altPath)

        if (!match0_ArrayIdxAlt.isEmpty) {
            val match0 = match0_ArrayIdxAlt(0).group(0) // Match 0 is an array index alternative [1,3,...]
            val after_match0 = match0_ArrayIdxAlt(0).after(0) // The rest (after match0) may be empty
            if (logger.isTraceEnabled()) logger.trace("match: " + match0 + ", after_match: " + after_match0)

            // Find each individual array index in the alternative
            val match1_ArrayIdxAltNames = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX.findAllMatchIn(match0).toList

            // Build the members of the OR query operator: .1<JPexpr>, .3<JPexpr>, etc.
            val orMembers = match1_ArrayIdxAltNames.map({ g => pre + "." + g.group(0) + after_match0 })
            if (logger.isDebugEnabled()) logger.debug("OR members: " + orMembers)

            // Run the translation of each OR member
            Some(new MongoQueryNodeOr(orMembers.map(om => trans(om, cond))))
        } else
            None
    }
}