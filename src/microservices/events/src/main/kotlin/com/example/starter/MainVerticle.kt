package com.example.starter

import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.AbstractVerticle
import io.vertx.core.Handler
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import io.vertx.kotlin.core.json.get
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

class MainVerticle : AbstractVerticle() {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun start() {
        val envStore = ConfigStoreOptions()
            .setType("env")
        val options = ConfigRetrieverOptions()
            .addStore(envStore)

        val retriever = ConfigRetriever.create(vertx, options)

        retriever.config.onSuccess { config ->
            log.info("Application started")

            val producerConfig = Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config["KAFKA_BROKERS"])
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            }

            // Настройка Kafka Consumer
            val consumerConfig = Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config["KAFKA_BROKERS"])
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
                put(ConsumerConfig.GROUP_ID_CONFIG, "consume_group")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }

            val producer = KafkaProducer.create<String, String>(vertx, producerConfig)
            val consumer = KafkaConsumer.create<String, String>(vertx, consumerConfig)

            consumer.handler { record ->
                log.info("Received Kafka message: Topic=${record.topic()}, Key=${record.key()}, Value=${record.value()}")
            }
            consumer.subscribe(setOf("user-events", "payment-events", "movie-events"))
            consumer.exceptionHandler { error ->
                log.error("Kafka consumer error", error)
            }

            val router = Router.router(vertx)
            router.route().handler(BodyHandler.create())

            router.get("/api/events/health").handler { ctx ->
                val healthResponse = JsonObject()
                    .put("status", true)

                ctx.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(healthResponse.encode())
            }

            router.post("/api/events/user").handler { ctx ->
                val body = ctx.body().asJsonObject() ?: throw IllegalArgumentException("Request body is required")

                val event = JsonObject()
                    .put(
                        "id",
                        "user-${body.getInteger("user_id")}-${body.getString("action")}-${System.currentTimeMillis()}"
                    )
                    .put("type", "user")
                    .put("timestamp", body.getString("timestamp"))
                    .put("payload", body)

                val record = KafkaProducerRecord.create<String, String>("user-events", event.toString())

                producer.send(record).onComplete { ar ->
                    if (ar.succeeded()) {
                        val response = JsonObject()
                            .put("status", "success")
                            .put("partition", ar.result().partition)
                            .put("offset", ar.result().offset)
                            .put("event", event)

                        ctx.response()
                            .setStatusCode(201)
                            .putHeader("Content-Type", "application/json")
                            .end(response.encode())
                    } else {
                        ctx.response()
                            .setStatusCode(500)
                            .putHeader("Content-Type", "application/json")
                            .end(JsonObject().put("error", ar.cause().message ?: "Internal server error").encode())
                    }
                }
            }

            router.post("/api/events/payment").handler { ctx ->
                val body = ctx.body().asJsonObject() ?: throw IllegalArgumentException("Request body is required")

                val event = JsonObject()
                    .put(
                        "id",
                        "payment-${body.getInteger("user_id")}-${body.getString("action")}-${
                            System.currentTimeMillis()
                        }"
                    )
                    .put("type", "user")
                    .put("timestamp", body.getString("timestamp"))
                    .put("payload", body)

                val record = KafkaProducerRecord.create<String, String>("payment-events", event.toString())

                producer.send(record).onComplete { ar ->
                    if (ar.succeeded()) {
                        val response = JsonObject()
                            .put("status", "success")
                            .put("partition", ar.result().partition)
                            .put("offset", ar.result().offset)
                            .put("event", event)

                        ctx.response()
                            .setStatusCode(201)
                            .putHeader("Content-Type", "application/json")
                            .end(response.encode())
                    } else {
                        ctx.response()
                            .setStatusCode(500)
                            .putHeader("Content-Type", "application/json")
                            .end(JsonObject().put("error", ar.cause().message ?: "Internal server error").encode())
                    }
                }
            }

            router.post("/api/events/movie").handler { ctx ->
                val body = ctx.body().asJsonObject() ?: throw IllegalArgumentException("Request body is required")

                val event = JsonObject()
                    .put(
                        "id",
                        "movie-${body.getInteger("user_id")}-${body.getString("action")}-${
                            System.currentTimeMillis
                                ()
                        }"
                    )
                    .put("type", "user")
                    .put("timestamp", body.getString("timestamp"))
                    .put("payload", body)

                val record = KafkaProducerRecord.create<String, String>("movie-events", event.toString())

                producer.send(record).onComplete { ar ->
                    if (ar.succeeded()) {
                        val response = JsonObject()
                            .put("status", "success")
                            .put("partition", ar.result().partition)
                            .put("offset", ar.result().offset)
                            .put("event", event)

                        ctx.response()
                            .setStatusCode(201)
                            .putHeader("Content-Type", "application/json")
                            .end(response.encode())
                    } else {
                        ctx.response()
                            .setStatusCode(500)
                            .putHeader("Content-Type", "application/json")
                            .end(JsonObject().put("error", ar.cause().message ?: "Internal server error").encode())
                    }
                }
            }
            vertx.createHttpServer()
                .requestHandler(router)
                .listen(config.get<Int>("PORT"));
        }
    }
}
