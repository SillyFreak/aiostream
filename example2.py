"""Run a TCP server that computes euclidean norm of vectors for its clients."""

import aiorx
import asyncio


# Constants

INSTRUCTIONS = """\
--------------------------------------
Compute the Euclidean norm of a vector
--------------------------------------

Enter each coordinate of the vector on a separate line, and add an empty line
at the end to get the result. Anything else will result in an error.

> """

ERROR = """\

-> Error ! Try again...

"""

RESULT = """\

-> Euclidean norm: {}

"""


# Client handler

async def euclidean_norm_handler(reader, writer):

    # Define lambdas
    strip =        lambda x: x.decode().strip()
    is_empty =     lambda x: x == ''
    square =       lambda x: float(x) ** 2
    write_cursor = lambda x: writer.write(b'> ')
    add =          lambda x, y: x + y
    square_root =  lambda x: x ** 0.5

    # Create awaitable
    observable = (aiorx
        .Observable(reader)
        .print_debug('string')
        .map(strip)
        .until(is_empty)
        .map(square)
        .print_debug('square')
        .action(write_cursor)
        .reduce(add, init=0)
        .map(square_root)
        .print_debug('norm')
    )

    # Loop over norm computations
    while not reader.at_eof():
        writer.write(INSTRUCTIONS.encode())
        try:
            result = await observable
        except ValueError:
            writer.write(ERROR.encode())
        else:
            writer.write(RESULT.format(result).encode())


# Main function

def run_server(bind='127.0.0.1', port=8888):

    # Start the server
    loop = asyncio.get_event_loop()
    coro = asyncio.start_server(euclidean_norm_handler, bind, port)
    server = loop.run_until_complete(coro)

    # Serve requests until Ctrl+C is pressed
    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


# Main execution

if __name__ == '__main__':
    run_server()