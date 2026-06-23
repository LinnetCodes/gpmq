"""Tests for subscriber decorators."""

from typing import Optional

import pytest

from gpmq.subscriber.subscriber import Subscriber
from typing import Any, Optional


class TestRunForeverMethodDecorator:
    """Tests for run_forever_method decorator."""

    def test_valid_signature_two_params(self):
        """Should accept function with (subscriber, paras) signature."""
        from gpmq.subscriber.decorators import run_forever_method

        @run_forever_method
        def my_run_forever(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
            pass

        assert getattr(my_run_forever, "_gpmq_run_forever", False) is True

    def test_valid_signature_no_annotations(self):
        """Should accept function without type annotations."""
        from gpmq.subscriber.decorators import run_forever_method

        @run_forever_method
        def my_run_forever(subscriber, paras):
            pass

        assert getattr(my_run_forever, "_gpmq_run_forever", False) is True

    def test_rejects_zero_params(self):
        """Should reject function with no parameters."""
        from gpmq.subscriber.decorators import run_forever_method

        with pytest.raises(TypeError, match="must accept exactly 2 parameters"):
            @run_forever_method
            def my_run_forever():
                pass

    def test_rejects_one_param(self):
        """Should reject function with only one parameter."""
        from gpmq.subscriber.decorators import run_forever_method

        with pytest.raises(TypeError, match="must accept exactly 2 parameters"):
            @run_forever_method
            def my_run_forever(subscriber):
                pass

    def test_rejects_three_params(self):
        """Should reject function with three parameters."""
        from gpmq.subscriber.decorators import run_forever_method

        with pytest.raises(TypeError, match="must accept exactly 2 parameters"):
            @run_forever_method
            def my_run_forever(subscriber, paras, extra):
                pass

    def test_rejects_wrong_second_param_name(self):
        """Should reject function where second parameter is not named 'paras'."""
        from gpmq.subscriber.decorators import run_forever_method

        with pytest.raises(TypeError, match="must be named 'paras'"):
            @run_forever_method
            def my_run_forever(subscriber, config):
                pass

    def test_rejects_non_none_return_annotation(self):
        """Should reject function with non-None return annotation."""
        from gpmq.subscriber.decorators import run_forever_method

        with pytest.raises(TypeError, match="return annotation must be None"):
            @run_forever_method
            def my_run_forever(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> str:
                return "ok"

    def test_returns_original_function(self):
        """Should return the original function unchanged."""
        from gpmq.subscriber.decorators import run_forever_method

        def my_run_forever(subscriber: Subscriber, paras: Optional[dict[str, Any]]) -> None:
            pass

        decorated = run_forever_method(my_run_forever)
        assert decorated is my_run_forever

    def test_second_param_case_insensitive(self):
        """Should accept second param named 'Paras' or 'PARAS' (case-insensitive)."""
        from gpmq.subscriber.decorators import run_forever_method

        @run_forever_method
        def my_run_forever_upper(subscriber, PARAS):
            pass

        assert getattr(my_run_forever_upper, "_gpmq_run_forever", False) is True
