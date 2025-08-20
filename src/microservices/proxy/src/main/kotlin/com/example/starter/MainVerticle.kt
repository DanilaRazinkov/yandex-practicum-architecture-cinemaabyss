package com.example.starter

import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.AbstractVerticle
import io.vertx.ext.web.Router
import io.vertx.ext.web.proxy.handler.ProxyHandler
import io.vertx.httpproxy.HttpProxy
import io.vertx.kotlin.core.json.get
import java.net.URI

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

            val moviesUri = URI.create(config.get<String>("MOVIES_SERVICE_URL"))
            val moviesProxy = HttpProxy.reverseProxy(proxyClient)
            moviesProxy.origin(moviesUri.port, moviesUri.host)

            val eventsUri = URI.create(config.get<String>("EVENTS_SERVICE_URL"))
            val eventsProxy = HttpProxy.reverseProxy(proxyClient)
            eventsProxy.origin(eventsUri.port, eventsUri.host)

            val proxyRouter = Router.router(vertx)

            proxyRouter
                .route("/health")
                .handler(ProxyHandler.create(monolithProxy))

            proxyRouter
                .route("/api/users*")
                .handler(ProxyHandler.create(monolithProxy))

            proxyRouter
                .route("/api/payments*")
                .handler(ProxyHandler.create(monolithProxy))


            proxyRouter
                .route("/api/movies*")
                .handler(ProxyHandler.create(moviesProxy))

            val proxyServer = vertx.createHttpServer()
            proxyServer.requestHandler(proxyRouter)
            proxyServer.listen(config.get<Int>("PORT"))
        }
    }
}
