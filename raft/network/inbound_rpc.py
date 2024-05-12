import logging
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread

import orjson
from raft.network.message_utils import send_message_to_socket, recv_message_from_socket, encode_json_message, decode_json_message
from raft.node.commands import COMMAND_TO_REQUEST_MODEL, COMMAND_IS_ASYNC

logger = logging.getLogger(__name__)


def handle_inbound_rpc_socket(command_recipient, socket):
    logger.debug('Starting inbound rpc handler')
    try:
        while True:
            msg = recv_message_from_socket(socket)
            try:
                decoded_message = decode_json_message(msg)
            except orjson.JSONDecodeError:
                logger.error('Failed to decode JSON message')
                send_message_to_socket(socket, encode_json_message({'error': 'Failed to decode JSON message'}))
                continue

            command = decoded_message.get("command")

            if not command:
                logger.error('No command in message')
                send_message_to_socket(socket, encode_json_message({'error': 'No command in message'}))
                continue

            func = getattr(command_recipient, command, None)
            if not func:
                logger.error('Command not found on recipient')
                send_message_to_socket(socket, encode_json_message({'error': 'Command not found on recipient'}))
                continue
            
            arguments = decoded_message.get("arguments", {})
            request = COMMAND_TO_REQUEST_MODEL[command].model_validate(arguments)
            is_async = COMMAND_IS_ASYNC[command]
            try:
                result_future = func(request=request)
                result = result_future.get()
                if is_async:
                    
                    # TODO-timeout: configure client-facing timeout
                    # This is how long we'll wait for an async
                    # operation to finish.
                    result = result.result(timeout=20)

                send_message_to_socket(socket, result.model_dump_json().encode())
            except Exception as e:
                logger.exception('Error executing command', exc_info=True)
                send_message_to_socket(socket, encode_json_message({'error': "Error executing command: " + str(e)}))
                continue
    except IOError:
        logger.warning('IOError waiting on inbound socket')
    except Exception as e:
        logger.exception('Exception in handle_inbound_rpc_socket', exc_info=True)
    finally:
        logger.warning('Closing inbound socket')
        socket.close()


def run_inbound_rpc_listener(addr, handler_func):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind(addr)
    sock.listen()

    logger.info(f'Listening on {addr}')
    try:
        while True:
            client_socket, client_addr = sock.accept()
            logger.debug(f'Accepted connection from {client_addr}')
            Thread(target=handler_func, args=(client_socket,), daemon=True).start()
    except Exception:
        logger.exception('Exception in run_inbound_rpc_listener', exc_info=True)
    finally:
        logger.info('Closing server listener socket')
        sock.close()