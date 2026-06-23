"""Tests for GPMQProgress base class."""

from gpmq.progress import GPMQProgress


class TestGPMQProgressCreate:
    def test_create_sets_max_value(self):
        progress = GPMQProgress()
        progress.create(10)
        assert progress.max_value == 10

    def test_create_resets_current_value(self):
        progress = GPMQProgress()
        progress.create(10)
        progress.update(5)
        progress.create(20)
        assert progress.current_value == 0
        assert progress.max_value == 20

    def test_initial_values_are_zero(self):
        progress = GPMQProgress()
        assert progress.max_value == 0
        assert progress.current_value == 0


class TestGPMQProgressUpdate:
    def test_update_sets_current_value(self):
        progress = GPMQProgress()
        progress.create(10)
        progress.update(5)
        assert progress.current_value == 5

    def test_update_replaces_value(self):
        progress = GPMQProgress()
        progress.create(10)
        progress.update(3)
        progress.update(7)
        assert progress.current_value == 7


class TestGPMQProgressLifecycle:
    def test_start_and_finish_do_not_error(self):
        """Base class start/finish should be no-ops."""
        progress = GPMQProgress()
        progress.create(10)
        progress.start()
        progress.finish()
