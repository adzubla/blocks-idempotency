package io.adzubla.blocks.idempotency.response;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import org.springframework.util.unit.DataSize;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Captures a handler's response for caching: status, headers (denylist
 * filtered, {@code Set-Cookie} always removed), and body. A body over {@code
 * maxBodySize} is dropped (not cached) rather than captured - the record is
 * still completed, just not replayable ({@code response_unavailable}).
 */
public final class ResponseCapture {

    private ResponseCapture() {
    }

    public static CachedResponse capture(ContentCachingResponseWrapper response, String[] headerDenylist, DataSize maxBodySize) {
        Set<String> denylist = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        denylist.addAll(Arrays.asList(headerDenylist));
        denylist.add("Set-Cookie");

        Map<String, List<String>> headers = new LinkedHashMap<>();
        for (String name : response.getHeaderNames()) {
            if (denylist.contains(name)) {
                continue;
            }
            headers.put(name, new ArrayList<>(response.getHeaders(name)));
        }

        byte[] content = response.getContentAsByteArray();
        byte[] body = content.length <= maxBodySize.toBytes() ? content : null;
        return new CachedResponse(response.getStatus(), headers, body);
    }
}
