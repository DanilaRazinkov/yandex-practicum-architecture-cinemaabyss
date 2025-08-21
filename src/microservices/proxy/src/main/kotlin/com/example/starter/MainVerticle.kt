package com.example.starter

import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.proxy.handler.ProxyHandler
import io.vertx.httpproxy.HttpProxy
import io.vertx.kotlin.core.json.get
import java.net.URI
import kotlin.random.Random

class MainVerticle : AbstractVerticle() {

    override fun start() {
        val envStore = ConfigStoreOptions()
            .setType("env")
        val options = ConfigRetrieverOptions()
            .addStore(envStore)

        val retriever = ConfigRetriever.create(vertx, options)

        retriever.config.onSuccess { config ->

            val proxyClient = vertx.createHttpClient()

            val monolithUri = URI.create(config.get<String>("MONOLITH_URL"))
            val monolithProxy = HttpProxy.reverseProxy(proxyClient)
            monolithProxy.origin(monolithUri.port, monolithUri.host)
            val monolithHandler = ProxyHandler.create(monolithProxy)

            val moviesUri = URI.create(config.get<String>("MOVIES_SERVICE_URL"))
            val moviesProxy = HttpProxy.reverseProxy(proxyClient)
            moviesProxy.origin(moviesUri.port, moviesUri.host)
            val movieHandler = ProxyHandler.create(moviesProxy)

            val eventsUri = URI.create(config.get<String>("EVENTS_SERVICE_URL"))
            val eventsProxy = HttpProxy.reverseProxy(proxyClient)
            eventsProxy.origin(eventsUri.port, eventsUri.host)
            val eventHandler = ProxyHandler.create(eventsProxy)

            val proxyRouter = Router.router(vertx)

            proxyRouter
                .route("/health").handler { ctx ->
                    val healthResponse = JsonObject()
                        .put("status", true)

                    ctx.response()
                        .setStatusCode(200)
                        .putHeader("Content-Type", "application/json")
                        .end(healthResponse.encode())
                }

            proxyRouter
                .route("/api/users*")
                .handler(monolithHandler)

            proxyRouter
                .route("/api/payments*")
                .handler(monolithHandler)

            proxyRouter
                .route("/api/events*")
                .handler(eventHandler)

            val migrationPercent = config.get<Int>("MOVIES_MIGRATION_PERCENT")

            proxyRouter
                .route("/api/movies*")
                .handler { ctx ->
                    val random = Random.nextInt(100)

                    if (random < migrationPercent) {
                        movieHandler.handle(ctx)
                    } else {
                        monolithHandler.handle(ctx)
                    }
                }

            val proxyServer = vertx.createHttpServer()
            proxyServer.requestHandler(proxyRouter)
            proxyServer.listen(config.get<Int>("PORT"))
        }
    }
}
