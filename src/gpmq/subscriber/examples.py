"""Message handler declaration and registry."""

from __future__ import annotations

import time
from typing import Any, Optional

from gpmq import Message
from gpmq.subscriber import Subscriber
from gpmq.subscriber.handler import message_handler
from gpmq.subscriber.decorators import worker_context_processor, run_forever_method
from gpmq.log import get_logger


@run_forever_method
def example_run_forever_method(subscriber: Subscriber, paras: Optional[dict[str, Any]] = None) -> None:
    logger = get_logger(subscriber.config.log_category)
    count = 0

    logger.info(f"enter run_forever_method with paras: {paras}")

    while True:
        count += 1
        logger.info(f"running forever with subscriber '{subscriber.name}', count: {count}")
        time.sleep(5)


@worker_context_processor
def example_worker_context_processor(context: dict[str, Any]) -> dict[str, Any]:
    logger = context["logger"] if "logger" in context else None
    if not logger:
        return dict()

    logger.info(f"example worker context processor of '{context['worker_id']}': "
                f"list items in worker context: ")

    for _k, _v in context.items():
        logger.info(f"example_worker_context_processor: context_keys: {_k}: {_v}")

    return dict()


@message_handler(["type_a"])
def on_msg_example_type_a(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    time.sleep(1)
    data = dict(msg.payload)
    data["retval"] = {"from": "on_msg_example_typa_a", "msg_type": "type_a", "result": "ok"}
    return data


@message_handler(["type_b"])
def on_msg_example_type_b(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    time.sleep(1)
    data = dict(msg.payload)
    data["retval"] = {"from": "on_msg_example_typa_b", "msg_type": "type_b", "result": "ok"}
    return data


@message_handler(["load_user_profile"])
def on_load_user_profile(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    data = dict()

    if context is None:
        raise Exception("missing worker context")
    _logger = context["logger"]
    _cfg_mgr = context["config_manager"]

    _logger.info(f"loading user profile for '{msg.payload['user_name']}'")
    time.sleep(1)  # mock a workload
    _logger.info(f"user profile for '{msg.payload['user_name']}' loaded")

    data["user_name"] = msg.payload["user_name"]
    data["user_id"] = "test_id"
    data["user_type"] = "test_account"

    return data


@message_handler(["load_user_posts", "load_user_activities"])
def on_load_user_data(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    data = dict()

    if context is None:
        raise Exception("missing worker context")
    _logger = context["logger"]
    _cfg_mgr = context["config_manager"]

    _logger.info(f"received message from '{msg.publisher}', type: '{msg.type}', payload: '{msg.payload}'")

    if msg.payload["user_name"] == "test_exception_user":  # stub for exception test
        _logger.error(f"user '{msg.payload['user_name']}' raised an exception when processing message '{msg.type}'")
        raise ValueError("this is only for exception test of gpmq worker process")

    if msg.type == "load_user_posts":
        _logger.info(f"loading user posts for '{msg.payload['user_name']}'")
        time.sleep(2)  # mock a workload
        _logger.info(f"user posts for '{msg.payload['user_name']}' loaded")
        data["posts"] = ["test_1", "test_2", "test_3"]

    elif msg.type == "load_user_activities":
        _logger.info(f"loading user activities for '{msg.payload['user_name']}'")
        time.sleep(10)  # mock a workload
        _logger.info(f"user activities for '{msg.payload['user_name']}' loaded")
        data["activities"] = ["test_4", "test_5", "test_6"]
    else:
        raise Exception(f"not supported message type '{msg.type}'")

    data["user_name"] = msg.payload["user_name"]
    data["user_id"] = "test_id"

    return data


@message_handler(["notify_mentioned", "notify_liked", "notify_message"])
def on_notify_user_interact(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    data = dict()

    if context is None:
        raise Exception("missing worker context")
    _logger = context["logger"]
    _cfg_mgr = context["config_manager"]

    if msg.type == "notify_mentioned":
        _logger.info(f"user 'test_user' mentioned '{msg.payload['user_name']}'")
        data["mentioned_from_user"] = "test_user"
    elif msg.type == "notify_liked":
        _logger.info(f"user 'test_user' liked '{msg.payload['user_name']}'")
        data["liked_from_user"] = "test_user"
    elif msg.type == "notify_message":
        _logger.info(f"user 'test_user' send '{msg.payload['user_name']}' a message 'hello, world'")
        data["msg_from_user"] = "test_user"
        data["message"] = "hello, world"
    else:
        raise Exception(f"not supported message type '{msg.type}'")

    return data


@message_handler(["notify_system_event"])
def on_notify_system_event(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    data = dict()

    if context is None:
        raise Exception("missing worker context")
    _logger = context["logger"]
    _cfg_mgr = context["config_manager"]

    if "heavy_workload" in msg.payload:
        _logger.info(f"user '{msg.payload['user_name']}' is notified with a system event of "
                     f"heavy workload '{msg.payload['heavy_workload']}'")
        time.sleep(msg.payload["heavy_workload"])
        _logger.info(f"user '{msg.payload['user_name']}' finished "
                     f"heavy workload of '{msg.payload['heavy_workload']}'")

    _logger.info(f"user '{msg.payload['user_name']}' is notified with a system event 'hello, new user'")
    data["user_name"] = msg.payload["user_name"]
    data["sys_evt_msg"] = "hello, new user"

    return data

@message_handler(["report_subscriber_status"])
def on_report_status(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    data = dict()

    if context is None:
        raise Exception("missing worker context")

    _logger = context["logger"]
    _cfg_mgr = context["config_manager"]

    if "raise_exception" in msg.payload and msg.payload["raise_exception"] == context["subscriber_name"]:
        _logger.error(f"subscriber '{context['subscriber_name']}' raised an exception "
                      f"when processing message '{msg.type}'")
        raise RuntimeError("this is only for exception test of gpmq worker process")

    _logger.info(f"subscriber '{context['subscriber_name']}' reported status to '{msg.payload['publisher']}': normal")

    data["subscriber_name"] = context['subscriber_name']
    data["worker_id"] = f"{context['subscriber_name']}-worker-{context['worker_id']}"
    data["status"] = "normal"

    return data

@message_handler(["msg_ping", "msg_pong"])
def on_msg_pingpong(msg: Message, context: Optional[dict[str, Any]] = None) -> Any:
    if context is None:
        raise Exception("missing worker context")
    _logger = context["logger"]

    if "publisher" not in context.keys():
        raise Exception("missing 'publisher' int context'")

    if "ttl" not in msg.payload:
        raise Exception("missing 'ttl' in message payload")

    if "from_subscriber" not in msg.payload:
        _from = msg.publisher
    else:
        _from = msg.payload['from_subscriber']

    _logger.info(f"subscriber '{context['subscriber_name']}' received message '{msg.type}' "
                 f"from '{msg.publisher}' of {_from}, ttl={msg.payload['ttl']}")

    _msg_type = "msg_ping" if msg.type == "msg_pong" else "msg_pong"
    _ttl = int(msg.payload["ttl"]) - 1
    if _ttl == 0:
        return True

    with context["publisher"] as _client:
        time.sleep(1.0)
        _client.send(_msg_type, {"ttl": _ttl, "from_subscriber": context['subscriber_name']}, correlation_id=msg.id)
        _logger.info(f"subscriber '{context['subscriber_name']}' send message '{_msg_type}' "
                     f"to message queue, ttl={_ttl}'")

    return True
