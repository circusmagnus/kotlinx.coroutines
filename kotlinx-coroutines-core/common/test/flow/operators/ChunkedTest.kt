/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlin.test.*
import kotlin.time.*

@ExperimentalTime
class ChunkedTest : TestBase() {

    @Test
    @Ignore
    fun testEmptyFlowChunking() = runTest {
        val emptyFlow = emptyFlow<Int>()
        val result = measureTimedValue {
            emptyFlow.universalChunked(10.seconds, Int.MAX_VALUE).toList()
        }

        assertTrue { result.value.isEmpty() }
        assertTrue { result.duration.inSeconds < 1 }
    }

    @ExperimentalTime
    @Test
    @Ignore
    fun testSingleFastElementChunking() = runTest {
        val fastFlow = flow { emit(1) }

        val result = measureTimedValue {
            fastFlow.universalChunked(10.seconds, Int.MAX_VALUE).toList()
        }

        assertTrue { result.value.size == 1 && result.value.first().contains(1) }
        assertTrue { result.duration.inSeconds < 1 }
    }

    @ExperimentalTime
    @Test
    @Ignore
    fun testMultipleFastElementsChunking() = runTest {
        val fastFlow = flow {
            for(i in 1..1000) emit(1)
        }

        val result = measureTimedValue {
            fastFlow.universalChunked(10.seconds, Int.MAX_VALUE).toList()
        }

        assertTrue { result.value.size == 1 && result.value.first().size == 1000 }
        assertTrue { result.duration.inSeconds < 1 }
    }

    @Test
    @Ignore
    fun testFixedTimeWindowChunkingWithZeroMinimumSize() = withVirtualTime {
        val intervalFlow = flow {
            delay(1500)
            emit(1)
            delay(1500)
            emit(2)
            delay(1500)
            emit(3)
        }
        val chunks = intervalFlow.chunked(2.seconds, minSize = 0).toList()

        assertEquals (3, chunks.size)
        assertTrue { chunks.all { it.size == 1 } }

        finish(1)
    }

    @Test
    @Ignore
    fun testDefaultChunkingWithFloatingWindows() = withVirtualTime {
        val intervalFlow = flow {
            delay(1500)
            emit(1)
            delay(1500)
            emit(2)
            delay(1500)
            emit(3)
        }
        val chunks = intervalFlow.chunked(2.seconds).toList()

        assertEquals (2, chunks.size)
        assertTrue { chunks.first().size == 2 && chunks[1].size == 1 }

        finish(1)
    }

    @Test
    @Ignore
    fun testRespectingMinValue() = withVirtualTime {
        val intervalFlow = flow {
            delay(1500)
            emit(1)
            delay(1500)
            emit(2)
            delay(1500)
            emit(3)
        }
        val chunks = intervalFlow.chunked(2.seconds, minSize = 3).toList()

        assertTrue { chunks.size == 1 }
        assertTrue { chunks.first().size == 3 }

        finish(1)
    }

    @Test
    @Ignore
    fun testRespectingMaxValueWithContinousWindows() = withVirtualTime {
        val intervalFlow = flow {
            delay(1500)
            emit(1)
            emit(2)
            emit(3)
            emit(4)
            delay(1500)
            emit(5)
            delay(1500)
            emit(6)
        }
        val chunks = intervalFlow.chunked(2.seconds, minSize = 0, maxSize = 3).toList()

        assertEquals(3, chunks.size)
        assertEquals(3, chunks.first().size)
        assertEquals(2, chunks[1].size)
        assertTrue { chunks[1].containsAll(listOf(4, 5)) }

        finish(1)
    }

    @Test
    @Ignore
    fun testRespectingMaxValueAndResetingTickerWithNonContinousWindows() = withVirtualTime {
        val intervalFlow = flow {
            delay(1500)
            emit(1)
            emit(2)
            emit(3)
            delay(1500)
            emit(4)
            emit(5)
            delay(1500)
            emit(6)
        }
        val chunks = intervalFlow.chunked(2.seconds, maxSize = 3).toList()

        assertEquals(2, chunks.size)
        assertEquals(3, chunks.first().size)
        assertEquals(3, chunks[1].size)
        assertTrue { chunks[1].containsAll(listOf(4, 5, 6)) }

        finish(1)
    }

    @Test
    @Ignore
    fun testSizeBasedChunking() = runTest {
        val flow = flow {
            for (i in 1..10) emit(i)
        }

        val chunks = flow.chunked(maxSize = 3).toList()

        assertEquals(4, chunks.size)
    }

    @Test
    @Ignore
    fun testSizeBasedChunkingWithMinSize() = runTest {
        val flow = flow {
            for (i in 1..10) emit(i)
        }

        val chunks = flow.chunked(maxSize = 3, minSize = 2).toList()

        assertEquals(3, chunks.size)
    }

    private val testFlow = flow {
        for(i in 1..100_000_000){
            emit(i)
        }

    }

    @Test
    fun benchChannelAsBuffer() = runTest {
        launch(Dispatchers.Default) {
            var emissionsCount = 0
            testFlow.chunkedChannelBuffer(100.milliseconds, 10_000_000)
                .onEach{ delay(10) }
                .onEach { emissionsCount += it.size }
                .count()
                .let { println("chunks: $it, total emissions: $emissionsCount") }
        }
    }

    @Test
    fun benchChannelAsBufferFast() = runTest {
        launch(Dispatchers.Default) {
            var emissionsCount = 0
//            flow {
//                emit(1)
//                emit(2)
//                delay(50)
//                for(i in 3..20) emit(i)
//            }

                testFlow.chunkedChannelBufferFast(100.milliseconds, 10_000_000)
                .onEach{ delay(10) }
                    .onEach { emissionsCount += it.size }
                .count()
                .let { println("chunks: $it, total emissions: $emissionsCount") }
        }
    }

    @Test
    fun benchUniversalChunked() = runTest {
        launch(Dispatchers.Default) {
            var emissionsCount = 0
            testFlow.universalChunked(interval = 100.milliseconds, size = 10_000_000)
                .onEach{ delay(10) }
                .onEach { emissionsCount += it.size }
                .count()
                .let { println("chunks: $it, total emissions: $emissionsCount") }
        }
    }

    @Test
    fun benchUniversalChunkedJustTimed() = runTest {
        launch(Dispatchers.Default) {
            var emissionsCount = 0
            testFlow.universalChunked(100.milliseconds, size = Int.MAX_VALUE)
                .onEach{ delay(10) }
                .onEach { emissionsCount += it.size }
                .count()
                .let { println("chunks: $it, total emissions: $emissionsCount") }
        }
    }

    @Test
    fun benchUniversalChunkedNatural() = runTest {
        launch(Dispatchers.Default) {
            var emissionsCount = 0
            testFlow.universalChunked(0.seconds, Int.MAX_VALUE)
                .onEach{ delay(10) }
                .onEach { emissionsCount += it.size }
                .count()
                .let { println("chunks: $it, total emissions: $emissionsCount") }
        }
    }

    @Test
    @Ignore
    fun checkUniversalChunkedSizeBased() = runTest {
        val tstFlow = flow { for (i in 1..1_000_000) emit(i) }
            launch(Dispatchers.Default) {
                var emissionsCount = 0
                tstFlow.universalChunked(interval = Duration.INFINITE, size = 1000)
                    .onEach{ delay(10) }
                    .onEach { println("chunk of size ${it.size}") }
                    .onEach { emissionsCount += it.size }
                    .count()
                    .let { println("chunks: $it, total emissions: $emissionsCount") }
            }
        }

    @Test
    fun benchUniversalChunkedSizeBased() = runTest {
        launch(Dispatchers.Default) {
            var emissionsCount = 0
            testFlow.universalChunked(interval = Duration.INFINITE, size = 10_000_000)
                .onEach{ delay(10) }
                .onEach { emissionsCount += it.size }
                .count()
                .let { println("chunks: $it, total emissions: $emissionsCount") }
        }
    }

    @Test
    @Ignore
    fun benchOldChunking() = runTest {
        launch(Dispatchers.Default) {
            var emissionsCount = 0
            testFlow.chunked(100.milliseconds, maxSize = 10_000_000)
                .onEach{ delay(10) }
                .onEach { emissionsCount += it.size }
                .count()
                .let { println("chunks: $it, total emissions: $emissionsCount") }
        }
    }

}