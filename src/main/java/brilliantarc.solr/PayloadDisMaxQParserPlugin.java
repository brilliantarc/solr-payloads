package brilliantarc.solr;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;

public class PayloadDisMaxQParserPlugin extends QParserPlugin {
    @Override
    public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest request) {
        return new PayloadDisMaxQParser(query, localParams, params, request);
    }

    @Override
    public void init(NamedList namedList) {
    }
}
