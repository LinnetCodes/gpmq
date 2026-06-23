"""Message handler declaration and registry."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable

from gpmq.exceptions import DuplicateMessageTypeError


def message_handler(message_types: list[str]) -> Callable:
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

    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)
        if 1 <= len(sig.parameters) <= 2:
            func._gpmq_message_types = message_types  # type: ignore[attr-defined]
            for name, param in sig.parameters.items():
                if name.lower() in {"msg", "message"}:
                    if param.annotation is inspect.Parameter.empty:
                        continue
                    if param.annotation != "Message":
                        raise TypeError(f"parameter '{name}' of a message handler '{func.__name__}' "
                                        f"must be of Message type, suggested message handler signature: \n"
                                        f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> "
                                        f"Any', or \n"
                                        f"'msg_handler_func(msg: Message) -> Any'")

                elif name.lower() in {"ctx", "context"}:
                    if param.annotation is inspect.Parameter.empty:
                        continue
                    if param.annotation != "Optional[dict[str, Any]]":
                        raise TypeError(f"parameter '{name}' of a message handler '{func.__name__}' "
                                        f"must be of type 'Optional[dict[str, Any]]', "
                                        f"suggested message handler signature: \n"
                                        f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> "
                                        f"Any', or \n"
                                        f"'msg_handler_func(msg: Message) -> Any'")
                else:
                    raise TypeError(f"invalid parameter '{name}' for message handler '{func.__name__}',"
                                    f"suggested message handler signature: \n"
                                    f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> Any', or \n"
                                    f"'msg_handler_func(msg: Message) -> Any'")
            return func
        else:
            raise TypeError(f"message handler only accept 1 or 2 parameters, got {len(sig.parameters)}, "
                            f"suggested message handler signature: \n"
                            f"'msg_handler_func(msg: Message, context: Optional[dict[str, Any]]) -> Any', or \n"
                            f"'msg_handler_func(msg: Message) -> Any'")
    return decorator


class HandlerRegistry:
    """Global registry for message handlers.

    Maps message type strings to handler callables.  Use
    :meth:`register` to add a handler (decorated with
    :func:`message_handler`) and :meth:`get_handler` to look one up.
    """

    _handlers: dict[str, Callable] = {}

    @classmethod
    def register(cls, func: Callable) -> None:
        """Register a decorated handler function.

        Args:
            func: A function decorated with :func:`message_handler`.

        Raises:
            AttributeError: If *func* has no ``_gpmq_message_types`` attribute.
            DuplicateMessageTypeError: If a type is already registered.
        """
        types_list: list[str] = getattr(func, "_gpmq_message_types", None)
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
    def get_handler(cls, message_type: str) -> Callable | None:
        """Look up the handler for a message type.

        Args:
            message_type: The message type string.

        Returns:
            The registered handler callable, or ``None`` if not found.
        """
        return cls._handlers.get(message_type)

    @classmethod
    def get_all_handlers(cls) -> dict[str, Callable]:
        """Return a copy of all registered handlers.

        Returns:
            Dict mapping message type string to handler callable.
        """
        return dict(cls._handlers)

    @classmethod
    def load_handler(cls, import_path: str) -> Callable:
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
        func = getattr(module, func_name)
        cls.register(func)
        return func

    @classmethod
    def clear(cls) -> None:
        """Remove all registered handlers.

        Primarily used in tests.
        """
        cls._handlers.clear()
