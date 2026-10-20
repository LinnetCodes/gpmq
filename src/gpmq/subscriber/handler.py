"""Message handler declaration and registry."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from typing import Any, Optional

from gpmq.exceptions import DuplicateMessageTypeError
from gpmq.models.message import Message

# Target type for the optional context parameter; built once so the validator
# can compare against real (non-string) annotations as well as the string form.
_CTX_TYPE: Any = Optional[dict[str, Any]]

# A message handler accepts ``(msg,)`` or ``(msg, context)`` and may return
# anything (see Worker._process_message for the return-value semantics), so
# the registry stores both shapes behind an ellipsis callable.
MessageHandler = Callable[..., Any]


def message_handler(message_types: list[str]) -> Callable[[MessageHandler], MessageHandler]:
    """Decorator that marks a function as a message handler.

    Attaches the declared message types as a ``_gpmq_message_types``
    attribute on the function.  The original function is returned unchanged.

    Args:
        message_types: List of message type strings this handler processes.

    Returns:
        The original function, unchanged.

    Raises:
        ValueError: If *message_types* is empty.
    """

    if not message_types:
        raise ValueError("message_types must not be empty")

    def decorator(func: MessageHandler) -> MessageHandler:
        sig = inspect.signature(func)
        params = list(sig.parameters.values())

        if not (1 <= len(params) <= 2):
            raise TypeError(
                f"message handler only accepts 1 or 2 parameters, got {len(params)}, "
                f"suggested message handler signature: \n"
                f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> Any', "
                f"or\n'msg_handler_func(msg: Message) -> Any'"
            )

        # --- 1st parameter: must be msg/message, annotated Message (or untyped) ---
        first = params[0]
        if first.name.lower() not in {"msg", "message"}:
            raise TypeError(
                f"the first parameter of message handler '{func.__name__}' must be named "
                f"'msg' or 'message' (got '{first.name}'), suggested message handler signature: \n"
                f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> Any', "
                f"or\n'msg_handler_func(msg: Message) -> Any'"
            )
        if first.annotation is not inspect.Parameter.empty:
            if not (first.annotation == "Message" or first.annotation is Message):
                raise TypeError(
                    f"parameter '{first.name}' of message handler '{func.__name__}' must be of "
                    f"Message type, got {first.annotation!r}, suggested message handler signature: \n"
                    f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> Any', "
                    f"or\n'msg_handler_func(msg: Message) -> Any'"
                )

        # --- 2nd parameter (optional): must be ctx/context, annotated Optional[dict[str, Any]] ---
        if len(params) == 2:
            second = params[1]
            if second.name.lower() not in {"ctx", "context"}:
                raise TypeError(
                    f"the second parameter of message handler '{func.__name__}' must be named "
                    f"'context' or 'ctx' (got '{second.name}'), suggested message handler signature: \n"
                    f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> Any', "
                    f"or\n'msg_handler_func(msg: Message) -> Any'"
                )
            if second.annotation is not inspect.Parameter.empty:
                if not (
                    second.annotation == "Optional[dict[str, Any]]"
                    or second.annotation == _CTX_TYPE
                ):
                    raise TypeError(
                        f"parameter '{second.name}' of message handler '{func.__name__}' must be of "
                        f"type 'Optional[dict[str, Any]]', got {second.annotation!r}, suggested message handler signature: \n"
                        f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> Any', "
                        f"or\n'msg_handler_func(msg: Message) -> Any'"
                    )

        func._gpmq_message_types = message_types  # type: ignore[attr-defined]
        return func
    return decorator


class HandlerRegistry:
    """Global registry for message handlers.

    Maps message type strings to handler callables.  Use
    :meth:`register` to add a handler (decorated with
    :func:`message_handler`) and :meth:`get_handler` to look one up.

    Note:
        This registry is a global class-level dict by design. GPMQ runs one
        Subscriber per process (the CLI creates a single Subscriber then
        ``run_forever()``), so process isolation already provides registry
        isolation. Spawned worker processes get a fresh interpreter and
        rebuild their own registry by re-importing handler modules.
    """

    _handlers: dict[str, MessageHandler] = {}

    @classmethod
    def register(cls, func: MessageHandler) -> None:
        """Register a decorated handler function.

        Args:
            func: A function decorated with :func:`message_handler`.

        Raises:
            AttributeError: If *func* has no ``_gpmq_message_types`` attribute.
            DuplicateMessageTypeError: If a type is already registered.
        """
        types_list: list[str] | None = getattr(func, "_gpmq_message_types", None)
        if not types_list:
            raise AttributeError(
                f"Function {func.__name__} is not decorated with @message_handler"
            )

        for msg_type in types_list:
            if msg_type in cls._handlers:
                raise DuplicateMessageTypeError(
                    subscriber_name=f"{cls._handlers[msg_type]}",
                    message_type=msg_type,
                )
            cls._handlers[msg_type] = func

    @classmethod
    def get_handler(cls, message_type: str) -> MessageHandler | None:
        """Look up the handler for a message type.

        Args:
            message_type: The message type string.

        Returns:
            The registered handler callable, or ``None`` if not found.
        """
        return cls._handlers.get(message_type)

    @classmethod
    def get_all_handlers(cls) -> dict[str, MessageHandler]:
        """Return a copy of all registered handlers.

        Returns:
            Dict mapping message type string to handler callable.
        """
        return dict(cls._handlers)

    @classmethod
    def load_handler(cls, import_path: str) -> MessageHandler:
        """Dynamically import a handler function and register it.

        Args:
            import_path: Dotted import path, e.g. ``"myapp.handlers.on_user"``.

        Returns:
            The imported handler function.

        Raises:
            ImportError: If the module or function cannot be imported.
        """
        parts = import_path.rsplit(".", 1)
        if len(parts) == 1:
            raise ImportError(f"Invalid handler import path: {import_path}")
        module_path, func_name = parts
        module = importlib.import_module(module_path)
        func: MessageHandler = getattr(module, func_name)
        cls.register(func)
        return func

    @classmethod
    def clear(cls) -> None:
        """Remove all registered handlers.

        Primarily used in tests.
        """
        cls._handlers.clear()
