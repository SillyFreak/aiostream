"""Combination operators."""

import asyncio
import builtins

from . import create
from ..aiter_utils import anext
from ..context_utils import AsyncExitStack
from ..core import operator, streamcontext

__all__ = ['chain', 'zip', 'map', 'merge', 'concat', 'flatten', 'switch',
           'concatmap', 'flatmap', 'switchmap']


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
async def base_combine(source, switch=False):
    streamers = {}

    async def enter(stack, source):
        streamer = await stack.enter_context(streamcontext(source))
        schedule(streamer)
        return streamer

    async def cleanup():
        for task, streamer in streamers.items():
            task.cancel()
            await streamer.aclose()
        streamers.clear()

    def schedule(streamer):
        task = asyncio.ensure_future(anext(streamer))
        streamers[task] = streamer

    async def completed():
        while streamers:
            done, _ = await asyncio.wait(
                list(streamers), return_when="FIRST_COMPLETED")
            for task in done:
                yield streamers.pop(task), task.result

    # Safe context
    async with AsyncExitStack() as stack:
        stack.callback(cleanup)

        # Initialize
        main_streamer = await enter(stack, source)

        # Loop over events
        async for streamer, getter in completed():

            # Get result
            try:
                result = getter()
            # End of stream
            except StopAsyncIteration:
                continue

            # Switch mecanism
            if switch and streamer is main_streamer:
                await cleanup()

            # Setup a new source
            if streamer is main_streamer:
                await enter(stack, result)
            # Simply yield the result
            else:
                yield result

            # Re-schedule streamer
            schedule(streamer)


@operator(pipable=True)
def flatten(source):
    """Given an asynchronous sequence of sequences, iterate over the element
    sequences in parallel.

    Element sequences are generated eagerly and iterated in parallel, yielding
    their elements interleaved as they arrive. Errors raised in the source or
    an element sequence are propagated.
    """
    return base_combine.raw(source, switch=False)


@operator(pipable=True)
def switch(source):
    """Given an asynchronous sequence of sequences, iterate over the most
    recent element sequence.

    Element sequences are generated eagerly, and closed once they are
    superseded by a more recent sequence. Errors raised in the source or an
    element sequence (that was not already closed) are propagated.
    """
    return base_combine.raw(source, switch=True)


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
    return flatten.raw(create.iterate.raw(sources))


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
    return flatten.raw(map.raw(source, func, *more_sources))


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
