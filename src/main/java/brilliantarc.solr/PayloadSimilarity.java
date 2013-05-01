package brilliantarc.solr;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.util.BytesRef;

/**
 * Decode the payload to calculate similarity.
 */
public class PayloadSimilarity extends DefaultSimilarity {
    @Override
    public float scorePayload(int doc, int start, int end, BytesRef payload) {
        if (payload.length > 0) {
            return PayloadHelper.decodeFloat(payload.bytes, payload.offset);
        } else {
            return 1.0f;
        }
    }
}
