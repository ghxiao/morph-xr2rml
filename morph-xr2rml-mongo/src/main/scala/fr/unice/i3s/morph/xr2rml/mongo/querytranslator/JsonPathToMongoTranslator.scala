package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeElemMatch
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeWhere
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeWhere
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotSupported
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCompare
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotExists
import scala.collection.mutable.Queue
import oracle.jrockit.jfr.Logger

/**
 * This object implements the algorithm that translates a JSONPath expression and a top-level condition
 * into a - possibly equivalent - MongoDB query.
 * It is used during the xR2RML-based rewriting of SPARQL queries into MongoDB.
 * To understand this code, you must have read and understood the algorithm first. Ask the author for the reference.
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
object JsonPathToMongoTranslator {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Regex for JSONPath expressions containing a concatenation of single field names (with tailing-dot or in array notation)
     * array indexes and wildcard, i.e. one of: .p, ["p"], [i], .* or [*].
     * Field alternatives (["q","r"]) and array index alternatives ([1,2]) are NOT allowed.
     */
    final val JSONPATH_PATH = """^(\.\p{Alnum}+|\["\p{Alnum}+"\]|\[\p{Digit}+\]|\[\*\]|\.\*)+""".r

    /**
     * Regex for JSONPath expressions containing a concatenation of single field names (with tailing-dot or in array notation)
     * and array indexes, i.e. one of: .p, ["p"] or [i]. E.g. '.p.q.r', '.p[10]["r"]'
     * '*' is NOT allowed. Field alternatives (["q","r"]) and array index alternatives ([1,2]) are NOT allowed.
     */
    final val JSONPATH_PATH_NS = """^(\.\p{Alnum}+|\["\p{Alnum}+"\]|\[\p{Digit}+\])+""".r

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
     *  Regex for a JSONPath field name in double-quoted in an alternatives: "q"
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
     *  Regex for a JSONPath wildcard with heading dot: .*
     */
    final val JSONPATH_WILDCARD_DOTTED = """^\.\*""".r

    /**
     *  Regex for a JSONPath wildcard in array notation: [*]
     */
    final val JSONPATH_WILDCARD_ARRAY_NOTATION = """^\[\*\]""".r

    /**
     *  Regex for a JSONPath JavaScript condition: [?(<JS_expression>)]
     *  This regex is not 100% safe. If the JS expression contains things like [( or )]
     *  then the behavior quite unpredictable.
     */
    final val JSONPATH_JS_BOOL_EXPRESSION = """^\[\?\((((?!\)\]).)+)\)\]""".r

    /**
     *  Regex for a JSONPath JavaScript condition: [(<JS_expression>)]
     *  This regex is not 100% safe. If the JS expression contains things like [( or )]
     *  then the behavior quite unpredictable.
     */
    final val JSONPATH_JS_NUM_EXPRESSION = """^\[\((((?!\)\]).)+)\)\]""".r

    /**
     * Regex for any JSONPAth JavaScript condition [(<JS_expression>)] at any place
     */
    final val JSONPATH_JS_NUM_EXPRESSION_ANYPLACE = """\[\(((?!\)\]).)+\)\]""".r

    /**
     * Entry point of the translation of a JSONPath expression with a top-level condition into a MongoDB query.
     * the resulting query is not optimized. To get it optimized, call the optimize method or use
     * the other trans() method.
     *
     * @param jpPath the JSONPath to translate, may be empty or null
     * @return a MongoQueryNode instance representing the top-level object of the MongoDB query.
     * The result may be null in case no rule matched.
     *
     */
    def trans(jpPath: String, cond: MongoQueryNodeCond): MongoQueryNode = {
        trans(jpPath, cond, false)
    }

    /**
     * Entry point of the translation of a JSONPath expression with a top-level condition into a MongoDB query.
     *
     * @param jpPath the JSONPath to translate, may be empty or null
     * @param optimize true if query must be optimized
     * @return a MongoQueryNode instance representing the top-level object of the MongoDB query.
     * The result may be null in case no rule matched.
     *
     */
    def trans(jpPath: String, cond: MongoQueryNodeCond, optimize: Boolean): MongoQueryNode = {
        val path = if (jpPath == null) "" else jpPath
        if (optimize)
            transJsonPathToMongo(path, cond).optimize
        else
            transJsonPathToMongo(path, cond)
    }

    /**
     * Application of the translation rules R0 to R9, to translate a JSONPath expression with a given top-level condition
     * into a MongoDB query. Refer to the algorithm for the meaning of each rule.
     *
     * @param jpPath the JSONPath to translate, may be empty or null
     * @return a MongoQueryNode instance representing the top-level object of the MongoDB query.
     * The result CAN'T be null, but a MongoQueryNodeNop is returned in case no rule matched.
     *
     */
    private def transJsonPathToMongo(jpPath: String, cond: MongoQueryNodeCond): MongoQueryNode = {
        if (logger.isTraceEnabled()) logger.trace("trans(" + jpPath + ", " + cond + ")")

        // Clean string: remove spaces, use only double-quotes
        val path = jpPath.trim().replace("'", "\"").replaceAll("""\p{Space}""", "")

        // ------ Rule r0
        if (path.isEmpty())
            return cond

        // ------ Rule r1
        if (path.startsWith("$.*") ||
            (path.startsWith("$[") && !path.startsWith("$[\"") && !path.startsWith("$['"))) { // $[i] nok, but $["p", "q"] ok 

            logger.error("Invalid JSONPath expression [" + path + "]: MongoDB root document cannot be an array")
            return new MongoQueryNodeNotSupported(path)
        }
        if (path.charAt(0) == '$') {
            if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matches rule r1")
            return trans(path.substring(1), cond)
        }

        // ------ Rule r2: Field alternative or array index alternative
        {
            // First, try to match <JPpath_ws> at the beginning of the path
            val match0_JPpath_ws = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(path).toList

            if (!match0_JPpath_ws.isEmpty) {
                val match0 = match0_JPpath_ws(0).group(0) // Match0 is a <JPpath_ws>
                val after_match0 = match0_JPpath_ws(0).after(0)

                // ------ Rule r2a:
                // trans(<JPpath_ws>["p","q",...]<JPexpr>, <cond>) :-
                //        OR( trans(<JPpath_ws>.p<JPexpr>, <cond>), trans(<JPpath_ws>.q<JPexpr>, <cond>), ... )
                var result = translateFieldAlternative(match0, after_match0.toString(), cond)
                if (result.isDefined) {
                    if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matched rule r2a")
                    return result.get
                }

                // ------ Rule r2b:
                // trans(<JPpath_ws>[1,3,...]<JPexpr>, <cond>) :-
                //        OR( trans(<JPpath_ws>.1<JPexpr>, <cond>), trans(<JPpath_ws>.3<JPexpr>, <cond>), ... )
                result = translateArrayIndexAlternative(match0, after_match0.toString(), cond)
                if (result.isDefined) {
                    if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matched rule r2b")
                    return result.get
                }
            }
        }

        // ------ Rule r3: Heading field alternative or array index alternative
        {
            // ------ Rule r3a:
            // trans(["p","q",...]<JPexpr>, <cond>) :-
            //        OR( trans(.p<JPexpr>, <cond>), trans(.q<JPexpr>, <cond>), ... )
            var result = translateFieldAlternative("", path, cond)
            if (result.isDefined) {
                if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matched rule r3a")
                return result.get
            }

            // ------ Rule r3b:
            // trans([1,3,...]<JPexpr>, <cond>) :-
            //        OR( trans(.1<JPexpr>, <cond>), 
            //            trans(.3<JPexpr>, <cond>), ... )
            result = translateArrayIndexAlternative("", path, cond)
            if (result.isDefined) {
                if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matched rule r3b")
                return result.get
            }
        }

        // ------ Rule r4: Heading JavaScript filter on array elements, e.g. $.[?(@.q)].r
        // 		  trans([?(<script_expr>)]<JPpath_alt_nw><JPpath_alt_filt>, <cond>) :-
        //			    ELEMMATCH(trans(<JPpath_alt_nw><JPpath_alt_filt>, <cond>), transJS(replaceAt(this, <script_expr>)))
        {
            // Find the JavaScript boolean expression
            val match0_JSexpr = JsonPathToMongoTranslator.JSONPATH_JS_BOOL_EXPRESSION.findAllMatchIn(path).toList
            if (!match0_JSexpr.isEmpty) {

                val match0 = match0_JSexpr(0).group(1) // Match 0 = [?(<script_expr>)], group(1) = <script_expr>
                val after_match0 = match0_JSexpr(0).after(0)

                // Unsupported case: $.p[?(@.q)].* or $.p[?(@.q)][*], because @.q means elements of p are documents but we apply a '*' to them.
                // But and $.p[?(@.q)].p.* are ok.
                if ((after_match0.toString.startsWith(".*") || after_match0.toString.startsWith("[*]"))) {
                    logger.warn("Unsupported JSONPath expression [" + path + "]: the JavaScript filter means that elements are documents, and the wildcard is not applicable to documents")
                    return new MongoQueryNodeNotSupported(path)
                }

                // Next, try to find the remaining part: it should not contain any script expression [()]
                if (!JsonPathToMongoTranslator.JSONPATH_JS_NUM_EXPRESSION_ANYPLACE.findAllMatchIn(after_match0).toList.isEmpty) {
                    logger.warn("Unsupported JSONPath expression [" + path + "]: a JavaScript filter [?(...)] cannot be followed by a JavaScript calculated index [(...)]")
                    return new MongoQueryNodeNotSupported(path)
                }

                // Build the ELEMMATCH query operator
                if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matches rule r4")
                val jsExprToMongoQ = JavascriptToMongoTranslator.transJS(replaceAt("this", match0))
                if (jsExprToMongoQ.isDefined) {
                    val members = List(trans(after_match0.toString, cond), jsExprToMongoQ.get)
                    if (logger.isTraceEnabled()) logger.trace("Rule 4, ELEMMATCH members: " + members.map(m => m.toString))
                    return new MongoQueryNodeElemMatch(members)
                } else {
                    logger.error("Rule r4: JS expression [" + replaceAt("this", match0) + "] could not be translated into a MongoDB query. Ignoring it.")
                    return trans(after_match0.toString, cond)
                }

            }
        }

        // ------ Rule r6: Calculated array index, e.g. $.p[(@.length - 1)
        //		  (a) trans(<JPpath_nw1>[(<num_expr>)]<JPpath_nw2>, <cond>) :- 
        //				AND(EXISTS(JPpath_nw1), WHERE('this<JPpath_nw1>[replaceAt("this<JPpath_nw1>", <num_expr>)]<JPpath_nw2> CONDJS(<cond>')))
        //		  (b) trans(<JPpath_nw1>[(<num_expr>)], <cond>) :- 
        //		        AND(EXISTS(JPpath_nw1), WHERE('this<JPpath_nw1>[replaceAt("this<JPpath_nw1>", <num_expr>)] CONDJS(<cond>')))
        {
            // First, try to match <JPpath_nw> at the beginning of the path: in fact we match
            // a <JPpath>, i.e. including the '*', and later on we'll check if there is a wildcard, so as to be 
            // able to report the warning about the wildcard used with a calculated array index

            val match0_JPpath_nw1 = JsonPathToMongoTranslator.JSONPATH_PATH.findAllMatchIn(path).toList
            if (!match0_JPpath_nw1.isEmpty) {

                val match0 = match0_JPpath_nw1(0).group(0) // Match0 is a <JPpath_nw1>
                val after_match0 = match0_JPpath_nw1(0).after(0)

                // Next, try to find the JavaScript calculated array index
                val match1_JSexpr = JsonPathToMongoTranslator.JSONPATH_JS_NUM_EXPRESSION.findAllMatchIn(after_match0).toList
                if (!match1_JSexpr.isEmpty) {

                    if (match0.toString.contains('*')) { // $.p.*[(<num_expr>)] not supported
                        logger.warn("Unsupported JSONPath expression [" + path + "]: wildcad not supported before a calculated array index")
                        return new MongoQueryNodeNotSupported(path)
                    }

                    val match1 = match1_JSexpr(0).group(1) // Match 1 = [(<num_expr>)], group(1) = <num_expr>
                    val after_match1 = match1_JSexpr(0).after(0)

                    if (after_match1.length == 0) {

                        // Rule r6a - Build the AND query operator
                        if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matches rule r6a")
                        val wherePart = new MongoQueryNodeWhere("this" + match0 + "[" + replaceAt("this" + match0, match1) + "]" + condJS(cond))
                        val andMembers = List(new MongoQueryNodeExists(match0), wherePart)
                        if (logger.isTraceEnabled()) logger.trace("Rule r6a, AND members: " + andMembers.map(m => m.toString))

                        return new MongoQueryNodeAnd(andMembers)
                    } else {
                        val match2_JPpath_nw2 = JsonPathToMongoTranslator.JSONPATH_PATH.findAllMatchIn(after_match1).toList
                        if (!match2_JPpath_nw2.isEmpty) {
                            val match2 = match2_JPpath_nw2(0).group(1)

                            if (match2.toString.contains('*')) { // $.p[(<num_expr>)].* not supported
                                logger.warn("Unsupported JSONPath expression [" + path + "]: wildcad not supported after a calculated array index")
                                return new MongoQueryNodeNotSupported(path)
                            }

                            // Rule r6b - Build the AND query operator
                            if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matches rule r6b")
                            val wherePart = new MongoQueryNodeWhere("this" + match0 + "[" + replaceAt("this" + match0, match1) + "]" + after_match1 + condJS(cond))
                            val andMembers = List(new MongoQueryNodeExists(match0), wherePart)
                            if (logger.isTraceEnabled()) logger.trace("Rule r6a, AND members: " + andMembers.map(m => m.toString))

                            return new MongoQueryNodeAnd(andMembers)
                        }
                    }

                }
            }
        }

        // ------ Rule r7: Heading wildcard
        //		  trans(.*<JPexpr>, <cond>) :- ELEMMATCH(trans(<JPexpr>, <cond>))
        {
            var fieldMatch: String = ""
            var afterMatch: String = ""

            // Try to match .* or [*]
            var matched = JsonPathToMongoTranslator.JSONPATH_WILDCARD_DOTTED.findAllMatchIn(path).toList
            if (matched.isEmpty)
                matched = JsonPathToMongoTranslator.JSONPATH_WILDCARD_ARRAY_NOTATION.findAllMatchIn(path).toList

            if (!matched.isEmpty) {
                if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matched rule r7")
                fieldMatch = matched(0).group(0)
                afterMatch = matched(0).after(0).toString
                if (logger.isTraceEnabled()) logger.trace("Rule 7, matched: " + fieldMatch + ", afterMatch: " + afterMatch);
                return new MongoQueryNodeElemMatch(trans(afterMatch, cond))
            }
        }

        // ------ Rule r8: Heading field name or array index
        //		  trans(.p<JPpath>, <cond>) :- FIELD(p) | trans(<JPpath>, <cond>)
        {
            var fieldMatch: String = ""
            var afterMatch: String = ""

            // Try to match .p
            var matched = JsonPathToMongoTranslator.JSONPATH_FIELD_NAME_DOTTED.findAllMatchIn(path).toList
            if (matched.isEmpty) {
                // Try to match ["p"]
                matched = JsonPathToMongoTranslator.JSONPATH_FIELD_NAME_ARRAY_NOTATION.findAllMatchIn(path).toList
                if (matched.isEmpty)
                    // Try to match [5]
                    matched = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX_ARRAY_NOTATION.findAllMatchIn(path).toList
            }

            if (!matched.isEmpty) {
                fieldMatch = matched(0).group(1)
                if (!fieldMatch.isEmpty) {
                    if (logger.isDebugEnabled()) logger.debug("JSONPath expression [" + path + "] matched rule r8")
                    afterMatch = matched(0).after(0).toString
                    if (logger.isTraceEnabled()) logger.trace("Rule 8, matched: " + fieldMatch + ", afterMatch: " + afterMatch);
                    return new MongoQueryNodeField(fieldMatch, trans(afterMatch, cond))
                }
            }
        }

        // ------ Rule r9: The path did not match any rule?
        logger.warn("JSONPath expression [" + path + "] did not match any rule. It is ignored.")
        new MongoQueryNodeNotSupported(path)
    }

    /**
     * Translates a field alternative (such as ["p","q",...]<JPexpr>) into an OR query operator:
     * OR( trans(.p<JPexpr>, <cond>), trans(.p<JPexpr>, <cond>), ... ).
     * It is used in the processing of rules 2a and 3a.
     *
     * @param prePath part of the JSONPath before the field alternative. It may be empty or null.
     * @param altPath a JSONPath that should start with a field alternative
     * @param cond the global condition (not null of equality)
     * @result a MongoQueryNode if altPath was a field alternative, none otherwise.
     * @throws MorphException in case of serious parsing issue (probably due to a miscall of this method)
     */
    def translateFieldAlternative(prePath: String, altPath: String, cond: MongoQueryNodeCond): Option[MongoQueryNode] = {
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
            if (logger.isTraceEnabled()) logger.trace("Field alternative: pre=" + pre + ", match0=" + match0 + ", after_match0=" + after_match0)

            // Find each individual field name in the alternative
            val match1_FieldAltNames = JsonPathToMongoTranslator.JSONPATH_FIELD_NAME_QUOTED.findAllMatchIn(match0).toList

            // Build the members of the OR query operator: .p<JPexpr>, .q<JPexpr>, etc.
            val orMembers = match1_FieldAltNames.map({ g => pre + "." + g.group(1) + after_match0 })
            if (logger.isTraceEnabled()) logger.trace("OR members: " + orMembers)

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
     * @throws MorphException in case of serious parsing issue (probably due to a miscall of this method)
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
            if (logger.isTraceEnabled()) logger.trace("Array index alternative: pre=" + pre + ", match0=" + match0 + ", after_match0=" + after_match0)

            // Find each individual array index in the alternative
            val match1_ArrayIdxAltNames = JsonPathToMongoTranslator.JSONPATH_ARRAY_IDX.findAllMatchIn(match0).toList

            // Build the members of the OR query operator: .1<JPexpr>, .3<JPexpr>, etc.
            val orMembers = match1_ArrayIdxAltNames.map({ g => pre + "." + g.group(0) + after_match0 })
            if (logger.isTraceEnabled()) logger.trace("OR members: " + orMembers)

            // Run the translation of each OR member
            Some(new MongoQueryNodeOr(orMembers.map(om => trans(om, cond))))
        } else
            None
    }

    /**
     * Replace any '@' occurrence with a replacement string
     */
    private def replaceAt(rep: String, jpPath: String): String = { jpPath.replace("@", rep) }

    /**
     * Translate a top-level condition into a JavaScript condition
     */
    private def condJS(cond: MongoQueryNodeCond): String = {
        cond.cond match {
            case MongoQueryNode.CondType.IsNotNull => " != null"
            case MongoQueryNode.CondType.Equals => " == " + cond.value
        }
    }
}