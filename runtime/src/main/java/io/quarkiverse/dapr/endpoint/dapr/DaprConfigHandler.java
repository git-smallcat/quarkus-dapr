package io.quarkiverse.dapr.endpoint.dapr;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dapr.actors.runtime.ActorRuntime;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

/**
 * DaprConfigHandler
 *
 * @author naah69
 * @date 22022-04-01 17:42:02
 */
public class DaprConfigHandler extends AbstractDaprHandler {
    private static final Logger log = LoggerFactory.getLogger(DaprConfigHandler.class);

    @Override
    public String subRoute() {
        return "config";
    }

    /**
     * Returns Dapr's configuration for Actors.
     *
     * @return Actor's configuration.
     * @throws IOException If cannot generate configuration.
     */
    @Override
    protected void get(RoutingContext event) {
        try {
            byte[] actorRuntimeConfig = ActorRuntime.getInstance().serializeConfig();
            event.end(Buffer.buffer(actorRuntimeConfig));
        } catch (IOException e) {
            log.error("Actor: get actor config error", e);
            event.fail(e);
        }
    }
}
