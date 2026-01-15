package streams.wal

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import org.slf4j.LoggerFactory
import streams.StreamsEventRouter
import streams.StreamsEventRouterConfiguration
import streams.events.RelationshipPayload
import streams.events.StreamsEvent
import streams.extensions.isDefaultDb
import streams.utils.JSONUtils


class WalEventRouter(private val config: Map<String, String>,
                     private val db: GraphDatabaseService,
                     private val log: Log): StreamsEventRouter(config, db, log) {

    private val transLog = LoggerFactory.getLogger("TransactionLog")

    override val eventRouterConfiguration: StreamsEventRouterConfiguration = StreamsEventRouterConfiguration
        .from(config, db.databaseName(), db.isDefaultDb(), log)

    private fun filterValidEvents(events: List<out StreamsEvent>): List<StreamsEvent> {
        return events.filter {
            if (it.payload is RelationshipPayload) {
                val payload = it.payload as RelationshipPayload
                payload.start.ids.isNotEmpty() && payload.end.ids.isNotEmpty()
            } else {
                true
            }
        }
    }

    private fun logEvent(event: StreamsEvent): String {
        val eventJson = JSONUtils.writeValueAsString(event)
        transLog?.info(eventJson)
        return eventJson
    }

    override fun sendEvents(
        topic: String,
        transactionEvents: List<out StreamsEvent>,
        config: Map<String, Any?>
    ) {
        filterValidEvents(transactionEvents).forEach {
            logEvent(it)
        }
    }

    override fun sendEventsSync(
        topic: String,
        transactionEvents: List<out StreamsEvent>,
        config: Map<String, Any?>
    ): List<Map<String, Any>> {
        return filterValidEvents(transactionEvents).map {
            val eventJson = logEvent(it)
            mapOf(
                "topic" to topic,
                "event" to eventJson
            )
        }
    }

    override fun start() {
    }

    override fun stop() {
    }


}