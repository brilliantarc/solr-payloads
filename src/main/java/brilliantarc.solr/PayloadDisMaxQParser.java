package brilliantarc.solr;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.payloads.MaxPayloadFunction;
import org.apache.lucene.search.payloads.PayloadFunction;
import org.apache.lucene.search.payloads.PayloadNearQuery;
import org.apache.lucene.search.payloads.PayloadTermQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DisMaxQParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;

import java.util.*;

/**
 * DisMaxQ-based query parser that incorporates payload values into the query.
 */
public class PayloadDisMaxQParser extends DisMaxQParser {

    /**
     * Identify the schema fields to search for payloads in the parser config.  For example:
     *
     * <pre>
     *     <str name="plf">
     *         category_en,keyword_en
     *     </str>
     * </pre>
     */
    public static final String PAYLOADS_FIELD_PARAM_NAME = "plf";

    private static final PayloadFunction PAYLOAD_FUNCTION = new MaxPayloadFunction();

    protected Set<String> payloadFields = new HashSet<>();
    protected float tiebreaker = 0.0f;

    public PayloadDisMaxQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        super(qstr, localParams, params, req);
    }

    /**
     * Expects a comma-separated list of fields under the "plf" parameter to parse for payloads.
     */
    @Override
    protected boolean addMainQuery(BooleanQuery query, SolrParams solrParams) throws SyntaxError {
        Map<String, Float> phraseFields = SolrPluginUtils.parseFieldBoosts(solrParams.getParams(DisMaxParams.PF));
        tiebreaker = solrParams.getFloat(DisMaxParams.TIE, 0.0f);

        String[] plfArray = solrParams.get(PAYLOADS_FIELD_PARAM_NAME, "").split("\\s+");
        Collections.addAll(payloadFields, plfArray);

        // Parse user input to DisMax queries...
        SolrPluginUtils.DisjunctionMaxQueryParser up = getParser(queryFields, DisMaxParams.QS, solrParams, tiebreaker);

        // For parsing sloppy phrases...
        SolrPluginUtils.DisjunctionMaxQueryParser pp = getParser(phraseFields, DisMaxParams.PS, solrParams, tiebreaker);

        parsedUserQuery = null;
        altUserQuery = null;

        String userQuery = getString();

        if (userQuery == null || userQuery.trim().length() == 0) {
            altUserQuery = getAlternateUserQuery(solrParams);
            if (altUserQuery == null) {
                return false;
            }
            query.add(altUserQuery, BooleanClause.Occur.MUST);
        } else {
            userQuery = SolrPluginUtils.partialEscape(SolrPluginUtils.stripUnbalancedQuotes(userQuery)).toString();
            userQuery = SolrPluginUtils.stripIllegalOperators(userQuery).toString();

            parsedUserQuery = getUserQuery(userQuery, up, solrParams);

            // Add the "tweaks" for payloads to the query
            query.add(rewriteQueryAsPayloadQuery(parsedUserQuery), BooleanClause.Occur.MUST);

            Query phrase = getPhraseQuery(userQuery, pp);
            if (null != phrase) {
                query.add(phrase, BooleanClause.Occur.SHOULD);
            }
        }

        return true;
    }

    /**
     * @param input the DisMaxQ user query
     * @return the query with payload objects
     */
    private Query rewriteQueryAsPayloadQuery(Query input) {
        Query output = handleQuery(input);
        output.setBoost(input.getBoost());
        return output;
    }

    private Query handleQuery(Query input) {
        if (input instanceof TermQuery) {
            return handleTermQuery((TermQuery) input);
        } else if (input instanceof PhraseQuery) {
            return handlePhraseQuery((PhraseQuery) input);
        } else if (input instanceof DisjunctionMaxQuery) {
            return handleDisMaxQuery((DisjunctionMaxQuery) input);
        } else if (input instanceof BooleanQuery) {
            return handleBooleanQuery((BooleanQuery) input);
        } else {
            return input;
        }
    }

    private Query handleTermQuery(TermQuery input) {
        Term term = input.getTerm();
        if (!payloadFields.contains(term.field())) {
            return input;
        }

        return new PayloadTermQuery(term, PAYLOAD_FUNCTION);
    }

    private Query handlePhraseQuery(PhraseQuery input) {
        Term[] terms = input.getTerms();
        int slop = input.getSlop();
        boolean inorder = false;

        if (terms.length > 0 && !payloadFields.contains(terms[0].field())) {
            return input;
        }

        SpanQuery[] clauses = new SpanQuery[terms.length];
        for (int i = 0; i < terms.length; i++) {
            clauses[i] = new PayloadTermQuery(terms[i], PAYLOAD_FUNCTION);
        }

        return new PayloadNearQuery(clauses, slop, inorder);
    }

    private Query handleDisMaxQuery(DisjunctionMaxQuery input) {
        DisjunctionMaxQuery result = new DisjunctionMaxQuery(tiebreaker);
        for (Query query : input) {
            result.add(rewriteQueryAsPayloadQuery(query));
        }

        return result;
    }

    private Query handleBooleanQuery(BooleanQuery input) {
        for (BooleanClause clause : input.clauses()) {
            clause.setQuery(rewriteQueryAsPayloadQuery(clause.getQuery()));
        }
        return input;
    }


    @Override
    public void addDebugInfo(NamedList<Object> debugInfo) {
        super.addDebugInfo(debugInfo);

        for (String field : this.payloadFields) {
            debugInfo.add("payloadField", field);
        }
    }
}
