package streams

import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import streams.extensions.execute
import streams.service.StreamsSinkService
import streams.service.StreamsStrategyStorage
import streams.utils.Neo4jUtils
import java.util.concurrent.atomic.AtomicReference

class NotInWriteableInstanceException(message: String): RuntimeException(message)

data class EventFlag(val ts: Long, val txId: Long, val txEventId: Long): Comparable<EventFlag> {
    fun connectToString(): String = "%s-%s-%s".format(ts, txId, txEventId)

    override fun compareTo(other: EventFlag): Int {
        return when {
            this.ts > other.ts - 30 * 1000 -> {
                1
            }
            this.ts < other.ts - 30 * 1000 -> {
                -1
            }
//            this.txId > other.txId -> {
//                1
//            }
//            this.txId < other.txId -> {
//                -1
//            }
//            this.txEventId > other.txEventId -> {
//                1
//            }
//            this.txEventId < other.txEventId -> {
//                -1
//            }
            else -> {
                0
            }
        }
    }

}

class StreamsEventSinkQueryExecution(private val db: GraphDatabaseAPI,
                                     private val log: Log,
                                     streamsStrategyStorage: StreamsStrategyStorage):
        StreamsSinkService(streamsStrategyStorage) {
    private val lastEventFlag = AtomicReference(EventFlag(-1, -1, -1))

    override fun write(query: String, params: Collection<Any>) {
        val currentEventFlag = AtomicReference(EventFlag(-1, -1, -1))
        val flagSet:HashSet<String> = HashSet()
        val paramsClean = params.filter { event ->
            val eventMap = event as? Map<String, Any>
            val meta = eventMap?.get("meta") as? Map<String, Any>
            val ts = meta?.get("timestamp")
            val txId = meta?.get("txId")
            val txEventId = meta?.get("txEventId")
            if (ts != null && txId != null && txEventId != null) {
                val eventFlag = EventFlag(ts.toString().toLong(), txId.toString().toLong(), txEventId.toString().toLong())
                val eventFlagStr = eventFlag.connectToString()
                if (eventFlag > lastEventFlag.get()) {
                    if (eventFlag > currentEventFlag.get()) {
                        currentEventFlag.set(eventFlag)
                    }
                    if (flagSet.contains(eventFlagStr)) {
//                        log.info("contain filter: %s", eventFlagStr)
                        false
                    } else {
                        flagSet.add(eventFlagStr)
                        true
                    }
                } else {
//                    log.info("less than last flag filter: %s", eventFlag)
                    false
                }
            } else {
//                log.info("null filter: %s", event)
                false
            }
        }

        log.info("input size: %s, clean size: %s", params.size, paramsClean.size)

        if (paramsClean.isEmpty()) return
        if (Neo4jUtils.isWriteableInstance(db)) {
//            log.info("query: $query, params: $params")
            db.execute(query, mapOf("events" to paramsClean)) {
                if (log.isDebugEnabled) {
                    log.debug("Query statistics:\n${it.queryStatistics}")
                }
            }
        } else {
            if (log.isDebugEnabled) {
                log.debug("Not writeable instance")
            }
            NotInWriteableInstanceException("Not writeable instance")
        }

        log.info("lastEventFlag: %s", lastEventFlag.get())
        log.info("currentEventFlag: %s", currentEventFlag.get())
        lastEventFlag.set(currentEventFlag.get())
    }
}
