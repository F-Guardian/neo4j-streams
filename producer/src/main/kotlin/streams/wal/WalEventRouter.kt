package streams.wal

import org.neo4j.logging.internal.LogService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import streams.StreamsEventRouter
import streams.config.StreamsConfig
import streams.events.RelationshipPayload
import streams.events.StreamsEvent
import streams.utils.JSONUtils

class WalEventRouter(logService: LogService, config: StreamsConfig, dbName: String): StreamsEventRouter(logService, config, dbName) {
    private val log: Logger = LoggerFactory.getLogger("TransactionLog")

    override fun sendEvents(topic: String, transactionEvents: List<out StreamsEvent>) {
        transactionEvents.filter {
            var flag = true
            if (it.payload is RelationshipPayload) {
                val payload = (it.payload as RelationshipPayload)
                if (payload.start.ids.isEmpty() || payload.end.ids.isEmpty()) {
                    flag = false
                }
            }
            flag
        }.forEach {
            val event = JSONUtils.writeValueAsString(it)
            log.info(event)
        }
    }

    override fun start() {
    }

    override fun stop() {
    }


}