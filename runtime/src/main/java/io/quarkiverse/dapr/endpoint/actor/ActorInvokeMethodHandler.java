package io.quarkiverse.dapr.endpoint.actor;

import io.dapr.actors.runtime.ActorRuntime;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;

/**
 * ActorInvokeMethodHandler
 *
 * @author naah69
 * @date 22022-04-01 17:42:02
 */
public class ActorInvokeMethodHandler extends AbstractDaprActorHandler {
    @Override
    public String subRoute() {
        return ":type/:id/method/:method";
    }

    /**
     * Handles API to invoke an actor's method.
     *
     * @param type Actor type.
     * @param id Actor Id.
     * @param method Actor method.
     * @param body Raw request body.
     * @return Raw response body.
     */
    @Override
    protected void put(RoutingContext event) {
        String type = event.pathParam("type");
        String id = event.pathParam("id");
        String method = event.pathParam("method");
        byte[] body = null;
        if (event.body().isEmpty()) {
            body = new byte[0];
        } else {
            body = event.body().buffer().getBytes();
        }
        byte[] result = ActorRuntime.getInstance().invoke(type, id, method, body).block();
        event.end(Buffer.buffer(result));
    }
}
