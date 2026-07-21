package io.adzubla.blocks.idempotency.response;

import io.adzubla.blocks.idempotency.model.CachedResponse;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

/** Replays a {@link CachedResponse} verbatim, flagged with the replay header. */
public final class ResponseReplayer {

    private ResponseReplayer() {
    }

    public static void replay(HttpServletResponse response, CachedResponse cached, String replayedHeaderName) throws IOException {
        response.setStatus(cached.status());
        cached.headers().forEach((name, values) -> values.forEach(value -> response.addHeader(name, value)));
        response.setHeader(replayedHeaderName, "true");
        if (cached.hasBody()) {
            response.getOutputStream().write(cached.body());
        }
    }
}
