"""GPMQProgress - base class for progress indicators."""


class GPMQProgress:
    """Progress indicator base class for batch async message processing.

    Provides a unified interface for progress tracking. Subclasses override
    methods to integrate with specific progress libraries (tqdm, rich, etc.).
    """

    def __init__(self) -> None:
        self._max_value: int = 0
        self._current_value: int = 0

    @property
    def max_value(self) -> int:
        """Total progress maximum value."""
        return self._max_value

    @property
    def current_value(self) -> int:
        """Current progress value."""
        return self._current_value

    def create(self, max_value: int) -> None:
        """Create progress indicator with max value."""
        self._max_value = max_value
        self._current_value = 0

    def start(self) -> None:
        """Start/display the progress indicator.

        The base records the minimal data state (current value reset to 0).
        Subclasses that add a display (tqdm, rich, etc.) should override this
        and call ``super().start()`` so data recording still happens.
        """
        self._current_value = 0

    def update(self, value: int) -> None:
        """Update current progress value."""
        self._current_value = value

    def finish(self) -> None:
        """Finish the progress indicator.

        The base records the minimal data state (current value set to the
        maximum). Subclasses that add a display should override this and call
        ``super().finish()`` so data recording still happens.
        """
        self._current_value = self._max_value
