package fr.unice.i3s.morph.xr2rml.mongo.querytranslator

import org.apache.log4j.Logger

import es.upm.fi.dia.oeg.morph.base.exception.MorphException
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryCondition
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionIsNull
import es.upm.fi.dia.oeg.morph.base.query.AbstractQueryConditionOr
import es.upm.fi.dia.oeg.morph.base.query.ConditionType
import es.upm.fi.dia.oeg.morph.base.query.IReference
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNode
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeAnd
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCond
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondEquals
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondFactory
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondIsNull
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondNotExists
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeCondNotNull
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeElemMatch
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeField
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeNotSupported
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeOr
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryNodeWhere
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryProjection
import fr.unice.i3s.morph.xr2rml.mongo.query.MongoQueryProjectionArraySlice

/**
 * This object implements the algorithm that translates a condition on a JSONPath expression
 * into a - possibly equivalent - MongoDB query.
 * It is used during the xR2RML-based rewriting of SPARQL queries into MongoDB.
 *
 * Prior to understand this code, one should read carefully the algorithm described in
 * "Mapping-based SPARQL access to a MongoDB database": https://hal.archives-ouvertes.fr/hal-01245883.
 *
 * @author Franck Michel (franck.michel@cnrs.fr)
 */
object JsonPathToMongoTranslator {

    val logger = Logger.getLogger(this.getClass().getName());

    /**
     * Regex for heading JSONPath expressions containing a concatenation of single field names (with tailing-dot or in array notation)
     * array indexes and wildcard, i.e. one of: .p, ["p"], [i], .* or [*].
     * Field alternatives (["q","r"]) and array index alternatives ([1,2]) are NOT allowed.
     */
    final val JSONPATH_PATH = """^(\.\p{Alnum}+|\["\p{Alnum}+"\]|\[\p{Digit}+\]|\[\*\]|\.\*)+""".r

    /**
     * Regex for heading JSONPath expressions containing a concatenation of single field names (with tailing-dot or in array notation)
     * and array indexes, i.e. one of: .p, ["p"] or [i]. E.g. '.p.q.r', '.p[10]["r"]'
     * '*' is NOT allowed. Field alternatives (["q","r"]) and array index alternatives ([1,2]) are NOT allowed.
     */
    final val JSONPATH_PATH_NS = """^(\.\p{Alnum}+|\["\p{Alnum}+"\]|\[\p{Digit}+\])+""".r

    /**
     *  Regex for a heading JSONPath field name with heading dot: .p
     *  The full match will be .p but the first capturing group will only p
     */
    //final val JSONPATH_FIELD_NAME_DOTTED = """^\.(\p{Alnum}+)""".r

    /**
     *  Regex for a heading JSONPath field name in array notation: ["p"]
     *  The full match will be ["p"] but the first capturing group will only p
     */
    //final val JSONPATH_FIELD_NAME_ARRAY_NOTATION = """^\["(\p{Alnum}+)"\]""".r

    /**
     *  Regex for a JSONPath field name in double-quoted in an alternatives: "q"
     *  The full match will be like "p" (including double-quotes), the first capturing group will only the p
     *  (without the double-quotes)
     */
    final val JSONPATH_FIELD_NAME_QUOTED = """"(\p{Alnum}+)"""".r

    /**
     * Regex for heading JSONPath field alternatives: ["q","r", ...]
     * The "?:" and the beginning of the group just indicates to not show it as part of the captured groups.
     */
    final val JSONPATH_FIELD_ALTERNATIVE = """^(\["\p{Alnum}+"(?:,"\p{Alnum}+")+\])""".r

    /**
     *  Regex for a heading JSONPath array index in array notation: [5]
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
     *  Regex for a heading JSONPath JavaScript filter condition: [(<JS_expression>)]
     *  This regex is not 100% safe. If the JS expression contains things like [( or )]
     *  then the behavior quite unpredictable.
     */
    final val JSONPATH_JS_NUM_EXPRESSION = """^\[\((((?!\)\]).)+)\)\]""".r

    /**
     * Regex for any JSONPAth JavaScript condition [(<JS_expression>)] at any place
     */
    final val JSONPATH_JS_NUM_EXPRESSION_ANYPLACE = """\[\(((?!\)\]).)+\)\]""".r

    /**
     *  Regex for a heading JSONPath array slice with a negative start index: [-10:]
     *  The full match will be [-10:] but the first capturing group will only -10
     */
    final val JSONPATH_ARRAY_SLICE_NEG_START = """^\[(-\p{Digit}+):\]""".r

    /**
     *  Regex for a heading JSONPath array slice with an end index: [:10] or [0:10]
     *  The full match will be [:10] or [0:10], but the first capturing group will only 10
     */
    final val JSONPATH_ARRAY_SLICE_END = """^\[0{0,1}:(\p{Digit}+)\]""".r

    /**
     * Entry point of the translation of a JSONPath expression with a top-level condition into a MongoDB query.
     * The resulting query is not optimized.
     *
     * @param cond and condition on a JSONPath expression to translate, must NOT be empty or null
     * @return a MongoQueryNode instance representing the top-level MongoDB query.
     * The result CANNOT be null, but a MongoQueryNodeNotSupported is returned in case no rule matched.
     *
     */
    def trans(cond: AbstractQueryCondition): MongoQueryNode = {
        trans(cond, List.empty)
    }

    /**
     * Entry point of the translation of a JSONPath expression with a top-level condition into a MongoDB query.
     * The resulting query is not optimized.
     *
     * @param cond and condition on a JSONPath expression to translate, must NOT be empty or null
     * @param projection set of projections to push in the MongoDB query (@todo not implemented)
     * @return a MongoQueryNode instance representing the top-level MongoDB query.
     * The result CANNOT be null, but a MongoQueryNodeNotSupported is returned in case no rule matched.
     *
     */
    def trans(cond: AbstractQueryCondition, projection: List[MongoQueryProjection]): MongoQueryNode = {
        var path =
            if (cond.hasReference) {
                val ref = cond.asInstanceOf[IReference].reference
                if (ref == null) "" else ref
            } else ""

        cond.condType match {
            case ConditionType.IsNull => {
                // isNull(ref) => OR(FIELD(ref) NOTEXISTS, FIELD(ref) ISNULL)
                val condIsNull = cond.asInstanceOf[AbstractQueryConditionIsNull]
                new MongoQueryNodeOr(List(
                    transJsonPathToMongo(path, new MongoQueryNodeCondNotExists, projection),
                    transJsonPathToMongo(path, new MongoQueryNodeCondIsNull, projection)
                ))
            }

            case ConditionType.Or => {
                val condOr = cond.asInstanceOf[AbstractQueryConditionOr]
                new MongoQueryNodeOr(condOr.members.map(c =>
                    transJsonPathToMongo(path, MongoQueryNodeCondFactory(c), projection)
                ))
            }

            case _ =>
                transJsonPathToMongo(path, MongoQueryNodeCondFactory(cond), projection)
        }
    }

    /**
     * Application of the translation rules R0 to R9, to translate a condition on a JSONPath expression
     * into a MongoDB query. Refer to the algorithm for the meaning of each rule.
     *
     * @param jpPath the JSONPath to translate, may be empty but NOT null
     * @param cond the isNotNull or Equals condition
     * @param projection applies to a field (R8) when we have detected an array slice notation (call from R5)
     * @return a MongoQueryNode instance representing the top-level MongoDB query.
     * The result CAN'T be null, but a MongoQueryNodeNotSupported is returned in case no rule matched.
     */
    private def transJsonPathToMongo(jpPath: String, cond: MongoQueryNodeCond, projection: List[MongoQueryProjection]): MongoQueryNode = {
        if (logger.isTraceEnabled()) logger.trace("trans([" + jpPath + "], [" + cond + "], [" + projection + "])")

        // Clean string: remove spaces, use only double-quotes
        val path = jpPath.trim().replace("'", "\"").replaceAll("""\p{Space}""", "")

        // ------ Rule R1
        // trans(, <cond>) -> COND(<cond>)
        if (path.isEmpty())
            return cond

        // ------ Rule R0
        // trans($.*<JP>, <cond>)    -> Error: root document cannot be an array in MD and wildcard is reserved for arrays
        // trans($[*]<JP>, <cond>)   -> Error: root document cannot be an array in MD
        // trans($[i,j,...], <cond>) -> Error: root document cannot be an array in MD
        // trans($[(<num_expr>)]<JP>, <cond>) -> Error: root document cannot be an array in MD
        if (path.startsWith("$.*") ||
            (path.startsWith("$[") && !path.startsWith("$[\"") && !path.startsWith("$['")) || // $[i] nok, but $["p", "q"] ok
            path.startsWith("$[(")) {
            logger.error("Invalid JSONPath expression [" + path + "]: MongoDB root document cannot be an array")
            return new MongoQueryNodeNotSupported(path)
        }

        // trans($, <cond>) -> {}
        // trans($<JP>, <cond>) -> trans(<JP>, <cond>)
        if (path.charAt(0) == '$') {
            if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matches rule R0")
            if (path.length == 1)
                return new MongoQueryNodeNotSupported(path)
            else
                return transJsonPathToMongo(path.substring(1), cond, projection)
        }

        // ------ Rule R2: Field alternative (a) or array index alternative (b)

        val match0_JPF = JsonPathToMongoTranslator.JSONPATH_PATH_NS.findAllMatchIn(path)
        val match0_JPF_list = match0_JPF.toList

        {
            // First, try to match <JP:F> at the beginning of the path
            if (!match0_JPF_list.isEmpty) {
                val match0 = match0_JPF_list(0).group(0) // Match0 is a <JP:F>
                val after_match0 = match0_JPF_list(0).after(0)

                // ------ Rule R2a:
                // trans(<JP:F>["p","q",...]<JP>, <cond>) ->
                //       OR(trans(<JP:F>.p<JP>, <cond>), trans(<JP:F>.q<JP>, <cond>), ...)
                var result = translateFieldAlternative(match0, after_match0.toString, cond, projection)
                if (result.isDefined) {
                    if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matched rule R2a")
                    return result.get
                }

                // ------ Rule R2b:
                // trans(<JP:F>[i,j,...]<JP>, <cond>) ->
                //       OR(trans(<JP:F>.i<JP>, <cond>), trans(<JP:F>.j<JP>, <cond>), ...)
                result = translateArrayIndexAlternative(match0, after_match0.toString(), cond, projection)
                if (result.isDefined) {
                    if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matched rule R2b")
                    return result.get
                }
            }
        }

        // ------ Rule R3: Heading field alternative or array index alternative
        {
            // ------ Rule R3a:
            // trans(["p","q",...]<JP>, <cond>) ->
            //       OR( trans(.p<JP>, <cond>), trans(.q<JP>, <cond>), ... )
            var result = translateFieldAlternative("", path, cond, projection)
            if (result.isDefined) {
                if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matched rule R3a")
                return result.get
            }

            // ------ Rule R3b:
            // trans([1,3,...]<JP>, <cond>) ->
            //       OR( trans(.1<JP>, <cond>), trans(.3<JP>, <cond>), ... )
            result = translateArrayIndexAlternative("", path, cond, projection)
            if (result.isDefined) {
                if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matched rule R3b")
                return result.get
            }
        }

        // ------ Rule R4: Heading JavaScript filter on array elements, e.g. $.p[?(@.q)].r, $.p[?(@[0] == 10)].*
        // 		  trans([?(<bool_expr>)]<JP>, <cond>) ->
        //			    ELEMMATCH(trans(<JP>, <cond>), transJS(<bool_expr>))
        {
            // Find the JavaScript boolean expression
            val match0_JSexpr = JsonPathToMongoTranslator.JSONPATH_JS_BOOL_EXPRESSION.findAllMatchIn(path).toList
            if (!match0_JSexpr.isEmpty) {

                val match0 = match0_JSexpr(0).group(1) // Match 0 = [?(<bool_expr>)], group(1) = <bool_expr>
                val after_match0 = match0_JSexpr(0).after(0)

                // Next, try to find the remaining part: it should not contain any filter expression [()]
                // This actually checks one part of R6a
                if (!JsonPathToMongoTranslator.JSONPATH_JS_NUM_EXPRESSION_ANYPLACE.findAllMatchIn(after_match0).toList.isEmpty) {
                    logger.warn("[" + path + "] matches rule R6a: unsupported JSONPath, a JavaScript filter [?(...)] cannot be followed by a JS calculated index [(...)]")
                    return new MongoQueryNodeNotSupported(path)
                }

                // Build the ELEMMATCH query operator
                if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matches rule R4")
                val transJsBoolExpr = JavascriptToMongoTranslator.transJS(replaceAt("this", match0))
                if (transJsBoolExpr.isDefined) {
                    val members = List(transJsonPathToMongo(after_match0.toString, cond, projection), transJsBoolExpr.get)
                    if (logger.isTraceEnabled()) logger.trace("Rule 4, ELEMMATCH members: " + members.map(m => m.toString))
                    return new MongoQueryNodeElemMatch(members)
                } else {
                    logger.error("Rule R4: JS expression [" + match0 + "] could not be translated into a MongoDB query. Ignoring it.")
                    return transJsonPathToMongo(after_match0.toString, cond, projection)
                }
            }
        }

        // ------ Rule R5: Array slice
        //    (a) trans(<JP:F>[-<start>:]<JP>, <cond>) ->     # last <start> elements
        //            trans(<JP:F>.*<JP>, <cond>) SLICE(dotNotation(<JP:F>), -<start>)
        //    (b) trans(<JP:F>[:<end>]<JP>, <cond>) ->        # the first <end> elements
        //            trans(<JP:F>.*<JP>, <cond>) SLICE(dotNotation(<JP:F>), <end>)
        {
            // First, try to match <JP:F> at the beginning of the path
            if (!match0_JPF_list.isEmpty) {
                val match0 = match0_JPF_list(0).group(0) // Match0 is a <JP:F>
                val after_match0 = match0_JPF_list(0).after(0)

                var after_match1: CharSequence = ""
                val arraySliceNode: Option[MongoQueryProjectionArraySlice] = {
                    // Case of a slice with negative start index
                    val match1_arraySliceStart = JsonPathToMongoTranslator.JSONPATH_ARRAY_SLICE_NEG_START.findAllMatchIn(after_match0).toList
                    if (!match1_arraySliceStart.isEmpty) {
                        val match1 = match1_arraySliceStart(0).group(1) // Match1 is a the start index e.g. -10 if array slice was [-10:]
                        if (logger.isTraceEnabled()) logger.trace("Rule 5a, negative array slice start index: " + match1)
                        after_match1 = match1_arraySliceStart(0).after(0) // may be empty
                        Some(new MongoQueryProjectionArraySlice(match0, match1))
                    } else {
                        // Case of a slice with end index
                        val match1_arraySliceEnd = JsonPathToMongoTranslator.JSONPATH_ARRAY_SLICE_END.findAllMatchIn(after_match0).toList
                        if (!match1_arraySliceEnd.isEmpty) {
                            val match1 = match1_arraySliceEnd(0).group(1) // Match1 is a the end index e.g. 10 if array slice was [:10]
                            if (logger.isTraceEnabled()) logger.trace("Rule 5b, array slice end index: " + match1)
                            after_match1 = match1_arraySliceEnd(0).after(0) // may be empty
                            Some(new MongoQueryProjectionArraySlice(match0, match1))
                        } else
                            None
                    }
                }
                if (arraySliceNode.isDefined)
                    return transJsonPathToMongo(match0.toString + ".*" + after_match1, cond, projection :+ arraySliceNode.get)
            }
        }

        // ------ Rule R6: Calculated array index, e.g. $.p[(@.length - 1)
        //    (a) trans($<JP1>[(<num_expr>)]<JP2>, <cond>) -> NOT_SUPPORTED		# if <JP1> contains a wildcard or a filter expression
        //    (b) trans(<JP:F>[(<num_expr>)], <cond>) ->
        //            AND(EXISTS(<JP:F>), WHERE('this<JP:F>[replaceAt("this<JP:F>", <num_expr>)] CONDJS(<cond>')))
        //    (c) trans(<JP1:F>[(<num_expr>)]<JP2:F>, <cond>) -> 
        //            AND(EXISTS(<JP1:F>),
        //            WHERE('this<JP1:F>[replaceAt("this<JP1:F>", <num_expr>)]<JP2:F> CONDJS(<cond>'))
        {
            // First, try to match <JP:F> at the beginning of the path: in fact we match
            // a <JP>, i.e. including the '*', and later on we'll check if there is a wildcard, so as to be 
            // able to report the warning about the wildcard used with a calculated array index

            val match0_JP = JsonPathToMongoTranslator.JSONPATH_PATH.findAllMatchIn(path).toList
            if (!match0_JP.isEmpty) {

                val match0 = match0_JP(0).group(0) // Match0 contains single field names, array indexes and wildcards
                val after_match0 = match0_JP(0).after(0)

                // Next, try to find the JavaScript calculated array index
                val match1_JSexpr = JsonPathToMongoTranslator.JSONPATH_JS_NUM_EXPRESSION.findAllMatchIn(after_match0).toList

                // if match1_JSexpr is empty we are in rule R7 or R8
                if (!match1_JSexpr.isEmpty) {

                    // R6a: trans($<JP1>[(<num_expr>)]<JP2>, <cond>) -> NOT_SUPPORTED if <JP1> contains a wildcard
                    // The case of R6a where <JP1> contains a filter expression [?(...)] is checked in rule R4.
                    if (match0.toString.contains('*')) { // $.p.*[(<num_expr>)] not supported
                        logger.warn("[" + path + "] matches rule R6a: unsupported JSONPath, a wildcad cannot be followed by a JS calculated array index")
                        return new MongoQueryNodeNotSupported(path)
                    }

                    val match1 = match1_JSexpr(0).group(1) // Match 1 = [(<num_expr>)], group(1) = <num_expr>
                    val after_match1 = match1_JSexpr(0).after(0)
                    if (after_match1.length == 0) {

                        // Rule R6b - Build the AND query operator
                        if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matches rule R6b")
                        val wherePart = new MongoQueryNodeWhere("this" + match0 + "[" + replaceAt("this" + match0, match1) + "]" + condJS(cond))
                        val andMembers = List(new MongoQueryNodeField(match0, new MongoQueryNodeCondExists), wherePart)
                        if (logger.isTraceEnabled()) logger.trace("Rule R6b, AND members: " + andMembers.map(m => m.toString))
                        return new MongoQueryNodeAnd(andMembers)
                    } else {
                        // Are we in rule R6c?
                        val match2_JP2F = JsonPathToMongoTranslator.JSONPATH_PATH.findAllMatchIn(after_match1).toList
                        if (!match2_JP2F.isEmpty) { // Match 2 is <JP2:F>
                            val match2 = match2_JP2F(0).group(1)

                            if (match2.toString.contains('*')) { // $.p[(<num_expr>)].* not supported
                                logger.warn("Unsupported JSONPath expression [" + path + "]: wildcad not supported after a calculated array index")
                                return new MongoQueryNodeNotSupported(path)
                            }

                            // Rule R6c - Build the AND query operator
                            if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matches rule r6b")
                            val wherePart = new MongoQueryNodeWhere("this" + match0 + "[" + replaceAt("this" + match0, match1) + "]" + after_match1 + condJS(cond))
                            val andMembers = List(new MongoQueryNodeField(match0, new MongoQueryNodeCondExists), wherePart)
                            if (logger.isTraceEnabled()) logger.trace("Rule R6c, AND members: " + andMembers.map(m => m.toString))
                            return new MongoQueryNodeAnd(andMembers)
                        }
                    }
                }
            }
        }

        // ------ Rule R7: Heading wildcard
        //	(a) trans(.*<JP>, <cond>) -> ELEMMATCH(trans(<JP>, <cond>))
        //	(b) trans([*]<JP>, <cond>) -> ELEMMATCH(trans(<JP>, <cond>))
        {
            var fieldMatch: String = ""
            var afterMatch: String = ""

            // Try to match .* or [*]
            var matched = JsonPathToMongoTranslator.JSONPATH_WILDCARD_DOTTED.findAllMatchIn(path).toList
            if (matched.isEmpty)
                matched = JsonPathToMongoTranslator.JSONPATH_WILDCARD_ARRAY_NOTATION.findAllMatchIn(path).toList

            if (!matched.isEmpty) {
                if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matched rule R7")
                fieldMatch = matched(0).group(0)
                afterMatch = matched(0).after(0).toString
                if (logger.isTraceEnabled()) logger.trace("Rule R7, matched: " + fieldMatch + ", afterMatch: " + afterMatch);
                return new MongoQueryNodeElemMatch(transJsonPathToMongo(afterMatch, cond, projection))
            }
        }

        // ------ Rule R8: Heading field name or array index
        //	(a) trans(.p<JP>, <cond>)    -> FIELD(p) trans(<JP>, <cond>)
        //  (b) trans(["p"]<JP>, <cond>) -> FIELD(p) trans(<JP>, <cond>)
        //	(c) trans([i]<JP>, <cond>)   -> FIELD(p) trans(<JP>, <cond>)
        // In fact we implement this in one step: .p["q"][1] => FIELD("p.q.1")  
        {
            if (!match0_JPF_list.isEmpty) {
                if (logger.isTraceEnabled()) logger.trace("JSONPath expression [" + path + "] matched rule r8")
                val match0 = match0_JPF_list(0).group(0) // Match0 is a <JP:F>
                val after_match0 = match0_JPF_list(0).after(0).toString()
                if (logger.isTraceEnabled()) logger.trace("Rule R8, matched: " + match0 + ", afterMatch: " + after_match0);
                return new MongoQueryNodeField(match0, List(transJsonPathToMongo(after_match0, cond, projection)), projection)
            }
        }

        // ------ Rule R9: The path did not match any rule?
        logger.warn("JSONPath expression [" + path + "] did not match any rule. It is ignored.")
        new MongoQueryNodeNotSupported(path)
    }

    /**
     * Translate a field alternative (such as ["p","q",...]<JPexpr>) into an OR query operator:
     * OR( trans(.p<JPexpr>, <cond>), trans(.p<JPexpr>, <cond>), ... ).
     *
     * @param prePath part of the JSONPath before the field alternative. It may be empty or null.
     * @param altPath a JSONPath that should start with a field alternative
     * @param cond the global condition (not null of equality)
     * @return a MongoQueryNode if altPath was a field alternative, none otherwise.
     * @throws es.upm.fi.dia.oeg.morph.base.exception.MorphException in case of serious parsing issue (probably due to a mistaken call to this method)
     */
    def translateFieldAlternative(prePath: String, altPath: String, cond: MongoQueryNodeCond, projection: List[MongoQueryProjection]): Option[MongoQueryNode] = {
        val pre =
            if (prePath == null || prePath.isEmpty()) ""
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
            Some(new MongoQueryNodeOr(orMembers.map(om => transJsonPathToMongo(om, cond, projection))))
        } else
            None
    }

    /**
     * Translates an array index alternative (such as [1,3,...]<JPexpr>) into an OR query operator:
     * OR( trans(.1<JPexpr>, <cond>), trans(.3<JPexpr>, <cond>), ... ).
     *
     * @param prePath part of the JSONPath that was before altPath. It may be empty or null.
     * @param altPath a JSONPath that should start with an array index alternative
     * @param cond the global condition (not null of equality)
     * @result a MongoQueryNode if altPath was an array index alternative, none otherwise.
     * @throws es.upm.fi.dia.oeg.morph.base.exception.MorphException in case of serious parsing issue (probably due to a mistaken call to this method)
     */
    private def translateArrayIndexAlternative(prePath: String, altPath: String, cond: MongoQueryNodeCond, projection: List[MongoQueryProjection]): Option[MongoQueryNode] = {
        val pre =
            if (prePath == null || prePath.isEmpty()) ""
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
            Some(new MongoQueryNodeOr(orMembers.map(om => transJsonPathToMongo(om, cond, projection))))
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
        cond match {
            case _: MongoQueryNodeCondNotNull => " != null"
            case c: MongoQueryNodeCondEquals => " == " + c.asInstanceOf[MongoQueryNodeCondEquals].value
        }
    }
}