/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:JvmMultifileClass
@file:JvmName("FlowKt")

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.internal.*
import kotlinx.coroutines.selects.*
import kotlin.jvm.*
import kotlin.time.*

private const val NO_MAXIMUM = -1

public fun <T> Flow<T>.chunked(maxSize: Int, minSize: Int = 1): Flow<List<T>> {
    require(minSize in 0 until maxSize)
    return flow {
        val accumulator = mutableListOf<T>()
        collect { value ->
            accumulator.add(value)
            if (accumulator.size == maxSize) emit(accumulator.drain())
        }
        if (accumulator.size >= minSize) emit(accumulator)
    }
}

@ExperimentalTime
public fun <T> Flow<T>.chunked(
    chunkDuration: Duration,
    minSize: Int = 1,
    maxSize: Int = NO_MAXIMUM
): Flow<List<T>> = chunked(chunkDuration.toDelayMillis(), minSize, maxSize)

public fun <T> Flow<T>.chunked(
    chunkDurationMs: Long,
    minSize: Int = 1,
    maxSize: Int = NO_MAXIMUM
): Flow<List<T>> {
    require(chunkDurationMs > 0)
    require(minSize >= 0)
    require(maxSize == NO_MAXIMUM || maxSize >= minSize)

    return if (minSize == 0 && maxSize == NO_MAXIMUM) chunkFixedTimeWindows(chunkDurationMs)
    else if (minSize == 0) chunkContinousWindows(chunkDurationMs, maxSize)
    else chunkFloatingWindows(chunkDurationMs, minSize, maxSize)
}

private fun <T> Flow<T>.chunkFixedTimeWindows(durationMs: Long): Flow<List<T>> = scopedFlow { downstream ->
    val upstream = produce(capacity = Channel.CHANNEL_DEFAULT_CAPACITY) {
        val ticker = Ticker(durationMs, this).apply { send(Ticker.Message.Start) }
        launch {
            for (tick in ticker) send(Signal.TimeIsUp)
        }
        collect { value -> send(Signal.NewElement(value)) }
        ticker.close()
    }
    val accumulator = mutableListOf<T>()

    for (signal in upstream) {
        when (signal) {
            is Signal.NewElement -> accumulator.add(signal.value)
            is Signal.TimeIsUp -> downstream.emit(accumulator.drain())
        }
    }
    if (accumulator.isNotEmpty()) downstream.emit(accumulator)
}

private fun <T> Flow<T>.chunkContinousWindows(durationMs: Long, maxSize: Int): Flow<List<T>> =
    scopedFlow { downstream ->
        val inbox: ReceiveChannel<T> = this@chunkContinousWindows.produceIn(this)
        val ticker = Ticker(durationMs, this).apply { send(Ticker.Message.Start) }
        val accumulator = mutableListOf<T>()

        whileSelect {
            inbox.onReceiveOrClosed.invoke { valueOrClosed ->
                val isOpen = !valueOrClosed.isClosed
                if (isOpen) {
                    accumulator.add(valueOrClosed.value)
                    if (accumulator.size == maxSize) {
                        ticker.send(Ticker.Message.Reset)
                        downstream.emit(accumulator.drain())
                        ticker.send(Ticker.Message.Start)
                    }
                }
                isOpen
            }
            ticker.onReceive.invoke {
                downstream.emit(accumulator.drain())
                true
            }
        }

        ticker.close()
        if (accumulator.isNotEmpty()) downstream.emit(accumulator)
    }

private fun <T> Flow<T>.chunkFloatingWindows(
    durationMs: Long,
    minSize: Int,
    maxSize: Int,
): Flow<List<T>> {

    return scopedFlow { downstream ->
        val upstream: ReceiveChannel<T> = this@chunkFloatingWindows.produceIn(this)
        val ticker = Ticker(durationMs, this)
        val accumulator = mutableListOf<T>()

        whileSelect {
            upstream.onReceiveOrClosed.invoke { valueOrClosed ->
                val isOpen = valueOrClosed.isClosed.not()
                if (isOpen) {
                    if (accumulator.isEmpty()) ticker.send(Ticker.Message.Start)
                    accumulator.add(valueOrClosed.value)
                    if (accumulator.size == maxSize) {
                        ticker.send(Ticker.Message.Reset)
                        downstream.emit(accumulator.drain())
                    }
                }
                isOpen
            }
            ticker.onReceive.invoke {
                if (accumulator.size >= minSize) downstream.emit(accumulator.drain())
                true
            }
        }

        ticker.close()
        if (accumulator.size >= minSize) downstream.emit(accumulator)
    }
}

private class Ticker(
    private val intervalMs: Long,
    scope: CoroutineScope,
    private val inbox: Channel<Message> = Channel(),
    private val ticks: Channel<Unit> = Channel()
) : SendChannel<Ticker.Message> by inbox, ReceiveChannel<Unit> by ticks {

    init {
        scope.processMessages()
    }

    private fun CoroutineScope.processMessages() = launch {
        var ticker = setupTicks()
        for (message in inbox) {
            when (message) {
                Message.Start -> ticker.start()
                Message.Reset -> {
                    ticker.cancel()
                    ticker = setupTicks()
                }
            }
        }
        ticker.cancel()
        ticks.cancel()
    }

    private fun CoroutineScope.setupTicks() = launch(start = CoroutineStart.LAZY) {
        while (true) {
            delay(intervalMs)
            ticks.send(Unit)
        }
    }

    sealed class Message {
        object Start : Message()
        object Reset : Message()
    }
}

private sealed class Signal<out T> {
    class NewElement<out T>(val value: T) : Signal<T>()
    object TimeIsUp : Signal<Nothing>()
}

private fun <T> MutableList<T>.drain() = toList().also { this.clear() }

private fun <T, R> List<T>.map(transform: (T) -> R): List<R> {
    val resultList = mutableListOf<R>()
    for (element in this) {
        val transformed = transform(element)
        resultList.add(transformed)
    }
    return resultList
}

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.chunkedChannelBuffer(time: Duration): Flow<List<T>> = scopedFlow { downstream ->
    val upstream = produceIn(this)
    val acc = mutableListOf<T>()
    whileSelect {
        upstream.onReceiveOrClosed { valueOrClosed ->
            if (valueOrClosed.isClosed) {
                downstream.emit(acc)
                false
            } else {
                acc.add(valueOrClosed.value)
                acc.addAll(upstream.drain())
                true
            }
        }

        onTimeout(time) { downstream.emit(acc.drain()); true }
    }
}

//////////////////////////////////////////////////////

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.chunkedNew(time: Duration, maxSize: Int = Int.MAX_VALUE): Flow<List<T>> {
    require(time >= Duration.ZERO && maxSize > 0)
    return when (time) {
        Duration.ZERO -> chunkedChannelNaturalBuffer()
        else -> chunkedChannelBufferFast(time, maxSize)
    }
}

@OptIn(ExperimentalTime::class)
private fun <T> Flow<T>.chunkedChannelNaturalBuffer(maxSize: Int = Int.MAX_VALUE): Flow<List<T>> =
    scopedFlow { downstream ->
        val accumulator = buffer(maxSize).produceIn(this)
        val chunk = mutableListOf<T>()
        val sink = Channel<List<T>>()

        whileSelect {
            sink.onSend(chunk.toList()) { chunk.clear(); accumulator.isClosedForReceive.not() }

            accumulator.onReceiveOrClosed { valueOrClosed ->
                if (valueOrClosed.isClosed) false
                else {
                    chunk.add(valueOrClosed.value)
                    true
                }
            }
        }
    }

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.chunkedChannelBufferFast(time: Duration, size: Int): Flow<List<T>> =
    scopedFlow { downstream ->
        val endOfStream = Job(parent = coroutineContext[Job])
        val accumulator = produce(capacity = size) {
            collect { value -> send(value) }
            endOfStream.complete()
        }

        whileSelect {
            endOfStream.onJoin {
                val chunk = accumulator.drain().takeUnless { it.isEmpty() }
                chunk?.let { downstream.emit(it) }
                false
            }

            onTimeout(time) {
                val chunk = accumulator.awaitFirstAndDrain()
                chunk.let { downstream.emit(it) }
                true
            }
        }
    }

///////////////////////////////////////////////////////

private suspend fun <T> ReceiveChannel<T>.awaitFirstAndDrain(): List<T> {
    val first = receiveOrClosed().takeIf { it.isClosed.not() }?.value ?: return emptyList()
    return drain(mutableListOf(first))
}

private tailrec fun <T> ReceiveChannel<T>.drain(acc: MutableList<T> = mutableListOf()): List<T> {
    val item = poll()
    return if (item == null) acc
    else {
        acc.add(item)
        drain(acc)
    }
}

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.chunkedNaive(time: Duration): Flow<List<T>> = scopedFlow { downstream ->
    val accumulator = mutableListOf<T>()
    launch {
        collect { value -> accumulator.add(value) }
    }

}

private enum class SemphoreSignal {
    MIN_SIZE_REACHED,
    MAX_SIZE_REACHED
}

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.universalChunked(interval: Duration, size: Int): Flow<List<T>> = scopedFlow { downstream ->
    val buffer = Channel<T>(size)
    val emitSemaphore = Channel<Unit>()
    val collectSemaphore = Channel<Unit>()

    launch {
        collect { value ->
            val hasCapacity = buffer.offer(value)
            if (!hasCapacity) {
                emitSemaphore.send(Unit)
                collectSemaphore.receive()
                buffer.send(value)
            }
        }
        emitSemaphore.close()
        buffer.close()
    }

    whileSelect {

        emitSemaphore.onReceiveOrClosed { valueOrClosed ->
            buffer.drain().takeIf { it.isNotEmpty() }?.let { downstream.emit(it) }
            val shouldCollectNextChunk = valueOrClosed.isClosed.not()
            if (shouldCollectNextChunk) collectSemaphore.send(Unit)
            else collectSemaphore.close()
            shouldCollectNextChunk
        }

        onTimeout(interval) {
            downstream.emit(buffer.awaitFirstAndDrain())
            true
        }
    }
}

//@OptIn(ExperimentalTime::class)
//public fun <T> Flow<T>.universalChunkedWithLambda(
//    interval: Duration,
//    size: Int,
//    shouldEmit: (timeLimitReached: Boolean, sizeLimitReached: Boolean) -> Boolean
//): Flow<List<T>> = scopedFlow { downstream ->
//    val buffer = Channel<T>(size)
//    val emitSemaphore = Channel<Unit>()
//    val collectSemaphore = Channel<Unit>()
//
//    launch {
//        collect { value ->
//            val hasCapacity = buffer.offer(value)
//            if (!hasCapacity) {
//                emitSemaphore.send(Unit)
//                collectSemaphore.receive()
//                buffer.send(value)
//            }
//        }
//        emitSemaphore.close()
//        buffer.close()
//    }
//    var sizeLimitReached = false
//    var timeLimitReached = false
//    whileSelect {
//        emitSemaphore.onReceiveOrClosed { valueOrClosed ->
//            sizeLimitReached = true
//            if (shouldEmit(timeLimitReached, sizeLimitReached)) {
//                buffer.drain().takeIf { it.isNotEmpty() }?.let { downstream.emit(it) }
//                sizeLimitReached = false
//                timeLimitReached = false
//            }
//            val shouldCollectNextChunk = valueOrClosed.isClosed.not()
//            if (shouldCollectNextChunk) collectSemaphore.send(Unit)
//            else collectSemaphore.close()
//            shouldCollectNextChunk
//        }
//
//        onTimeout(interval) {
//            timeLimitReached = true
//            if (shouldEmit(timeLimitReached, sizeLimitReached)) {
//                val chunk = buffer.awaitFirstAndDrain()
//                val canContinue = chunk != null
//                if(canContinue) downstream.emit(chunk!!)
//            } else true
////            val chunk = buffer.awaitFirstAndDrain()
////            val canContinue = chunk != null
////            if(canContinue && shouldEmit(timeLimitReached, sizeLimitReached)) {
////                downstream.emit(chunk!!)
////                timeLimitReached = false
////                sizeLimitReached = false
////            }
////            canContinue
//        }
//    }
//}

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.universalChunkedNaturalBuffering(maxSize: Int): Flow<List<T>> = scopedFlow { downstream ->
    val buffer = buffer(maxSize).produceIn(this)
    var isCollecting = true
    while (isCollecting) {
        buffer.awaitFirstAndDrain()?.let { downstream.emit(it) } ?: apply { isCollecting = false }
    }
}

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.universalChunkedJustTimed(duration: Duration): Flow<List<T>> = scopedFlow { downstream ->
    val buffer = buffer(Channel.UNLIMITED).produceIn(this)
    var isCollecting = true
    while (isCollecting) {
        delay(duration)
        buffer.awaitFirstAndDrain()?.let { downstream.emit(it) } ?: apply { isCollecting = false }
    }
}

@OptIn(ExperimentalTime::class)
public fun <T> Flow<T>.universalChunkedSizeBased(maxSize: Int): Flow<List<T>> = flow {
    val buffer = mutableListOf<T>()
    collect { value ->
        buffer.add(value)
        if (buffer.size == maxSize) emit(buffer.drain())
    }
    if (buffer.isNotEmpty()) emit(buffer)
}