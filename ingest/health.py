class HealthState:
    """
    Tracks the health state of the ingestion system.
    """

    def __init__(self):
        """
        Initialize the health state with default values.
        """
        self.redis_ready: bool = False
        self.last_fetch_ok: bool = False


# TODO: expose it somewhere for docker healthchecks
health = HealthState()
