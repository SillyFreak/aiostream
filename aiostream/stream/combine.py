"""Combination operators."""

import asyncio
import builtins

from . import create
from ..aiter_utils import anext
from ..context_utils import AsyncExitStack
from ..core import operator, streamcontext

__all__ = ['chain', 'zip', 'map', 'merge', 'concatmap', 'flatmap', 'switchmap']


@operator(pipable=True)
async def concat(source):
    """Given an asynchronous sequence of sequences, iterate over the element
    sequences in order.

    After one element sequence is exhausted, the next sequence is generated.
    Errors raised in the source or an element sequence are propagated.
    """
    async with streamcontext(source) as streamer:
        async for iterator in streamer:
            subsource = create.iterate.raw(iterator)
            async with streamcontext(subsource) as substreamer:
                async for item in substreamer:
                    yield item


@operator(pipable=True)
async def flat(source):
    """Given an asynchronous sequence of sequences, iterate over the element
    sequences in parallel.

    Element sequences are generated eagerly and iterated in parallel, yielding
    their elements interleaved as they arrive. Errors raised in the source or
    an element sequence are propagated.
    """
    streamers = {}

    async def cleanup():
        for task in streamers:
            task.cancel()

    async with AsyncExitStack() as stack:
        # Add cleanup
        stack.callback(cleanup)
        # Initialize
        streamer = await stack.enter_context(streamcontext(source))
        streamer_task = asyncio.ensure_future(anext(streamer))
        streamers[streamer_task] = streamer
        # Loop over events
        while streamers:
            done, _pending = await asyncio.wait(
                list(streamers), return_when="FIRST_COMPLETED")

            for task in done:
                try:
                    result = task.result()
                except StopAsyncIteration:
                    # End of stream
                    streamers.pop(task)
                    continue

                if task is streamer_task:
                    streamers.pop(task)
                    # Setup a new source
                    substreamer = await stack.enter_context(streamcontext(result))
                    substreamer_task = asyncio.ensure_future(anext(substreamer))
                    streamers[substreamer_task] = substreamer
                    # Schedule next anext
                    streamer_task = asyncio.ensure_future(anext(streamer))
                    streamers[streamer_task] = streamer
                else:
                    substreamer = streamers.pop(task)
                    # Simply yield a result
                    yield result
                    # Schedule next anext
                    substreamer_task = asyncio.ensure_future(anext(substreamer))
                    streamers[substreamer_task] = substreamer


@operator(pipable=True)
async def switch(source):
    """Given an asynchronous sequence of sequences, iterate over the most
    recent element sequence.

    Element sequences are generated eagerly, and closed once they are
    superseded by a more recent sequence. Errors raised in the source or an
    element sequence (that was not already closed) are propagated.
    """
    streamer_task = None
    substreamer_task = None

    async def cleanup():
        if streamer_task:
            streamer_task.cancel()
        if substreamer_task:
            substreamer_task.cancel()

    async with AsyncExitStack() as stack:
        # Add cleanup
        stack.callback(cleanup)
        # Initialize
        streamer = await stack.enter_context(streamcontext(source))
        streamer_task = asyncio.ensure_future(anext(streamer))
        substreamer = None
        # Loop over events
        while streamer_task or substreamer_task:
            wait_tasks = filter(None, (streamer_task, substreamer_task))
            done, _ = await asyncio.wait(wait_tasks, return_when="FIRST_COMPLETED")

            # Substreamer event
            if substreamer_task in done:
                try:
                    yield substreamer_task.result()
                except StopAsyncIteration:
                    # await substreamer.aclose()
                    await substreamer.__aexit__(None, None, None)
                    substreamer_task = None
                else:
                    substreamer_task = asyncio.ensure_future(anext(substreamer))

            # Streamer event
            if streamer_task in done:
                try:
                    subsource = streamer_task.result()
                except StopAsyncIteration:
                    streamer_task = None
                else:
                    # Clean up
                    if substreamer:
                        # await substreamer.aclose()
                        await substreamer.__aexit__(None, None, None)
                    if substreamer_task:
                        substreamer_task.cancel()
                    # New substream
                    streamer_task = asyncio.ensure_future(anext(streamer))
                    substreamer = await stack.enter_context(streamcontext(subsource))
                    substreamer_task = asyncio.ensure_future(anext(substreamer))


@operator(pipable=True)
def chain(*sources):
    """Chain asynchronous sequences together, in the order they are given.

    Note: the sequences are not iterated until it is required,
    so if the operation is interrupted, the remaining sequences
    will be left untouched.
    """
    return concat.raw(create.iterate.raw(sources))


@operator(pipable=True)
async def zip(*sources):
    """Combine and forward the elements of several asynchronous sequences.

    Each generated value is a tuple of elements, using the same order as
    their respective sources. The generation continues until the shortest
    sequence is exhausted.

    Note: the different sequences are awaited in parrallel, so that their
    waiting times don't add up.
    """
    async with AsyncExitStack() as stack:
        # Handle resources
        streamers = [await stack.enter_context(streamcontext(source))
                     for source in sources]
        # Loop over items
        while True:
            try:
                coros = builtins.map(anext, streamers)
                items = await asyncio.gather(*coros)
            except StopAsyncIteration:
                break
            else:
                yield tuple(items)


@operator(pipable=True)
async def map(source, func, *more_sources):
    """Apply a given function to the elements of one or several
    asynchronous sequences.

    Each element is used as a positional argument, using the same order as
    their respective sources. The generation continues until the shortest
    sequence is exhausted. The function can either be synchronous or
    asynchronous.

    Note: the different sequences are awaited in parallel, so that their
    waiting times don't add up.
    """
    iscorofunc = asyncio.iscoroutinefunction(func)
    if more_sources:
        source = zip(source, *more_sources)
    async with streamcontext(source) as streamer:
        async for item in streamer:
            if not more_sources:
                item = (item,)
            result = func(*item)
            if iscorofunc:
                result = await result
            yield result


@operator(pipable=True)
def merge(*sources):
    """Merge several asynchronous sequences together.

    All the sequences are iterated simultaneously and their elements
    are forwarded as soon as they're available. The generation continues
    until all the sequences are exhausted.
    """
    return flat.raw(create.iterate.raw(sources))


@operator(pipable=True)
def concatmap(source, func, *more_sources):
    """Apply a given function that returns a sequence to the elements of one or
    several asynchronous sequences, and iterate over the returned sequences in
    order.

    The function is applied as described in `map`, and can return an iterable
    or an asynchronous sequence. After one sequence is exhausted, the next
    sequence is generated. Errors raised in a source or output sequence are
    propagated.
    """
    return concat.raw(map.raw(source, func, *more_sources))


@operator(pipable=True)
def flatmap(source, func, *more_sources):
    """Apply a given function that returns a sequence to the elements of one or
    several asynchronous sequences, and iterate over the returned sequences in
    parallel.

    The function is applied as described in `map`, and can return an iterable
    or an asynchronous sequence. Sequences are generated eagerly and
    iterated in parallel, yielding their elements interleaved as they arrive.
    Errors raised in a source or output sequence are propagated.
    """
    return flat.raw(map.raw(source, func, *more_sources))


@operator(pipable=True)
def switchmap(source, func, *more_sources):
    """Apply a given function that returns a sequence to the elements of one or
    several asynchronous sequences, and iterate over the most recent sequence.

    The function is applied as described in `map`, and can return an iterable
    or an asynchronous sequence. Sequences are generated eagerly, and closed
    once they are superseded by a more recent sequence. Errors raised in a
    source or output sequence (that was not already closed) are propagated.
    """
    return switch.raw(map.raw(source, func, *more_sources))
