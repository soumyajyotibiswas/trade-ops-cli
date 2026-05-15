# ruff: noqa: E402

"""
Defines the client profile for the trading program.
"""

import sys
from pathlib import Path
from typing import Any

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))


from src.program_constants import BUFFER_MARGIN
from src.program_helpers import setup_logging

log = setup_logging("program_client_profile")


def _is_neo_client(client: Any) -> bool:
    """Return True when the SDK object looks like a Kotak Neo client."""
    return "NeoWebSocket" in dir(client)


def _margin_after_buffer(margin: float) -> float:
    """Apply the safety buffer while preserving the legacy minimum margin."""
    available_margin = margin - BUFFER_MARGIN
    if available_margin <= 0:
        return 1
    return float(round(available_margin, 2))


class ClientProfile:
    """
    Represents the client profile for the trading program.
    """

    def __init__(self, client: Any) -> None:
        """Store the broker SDK client used by the profile reader."""
        self.client = client

    def get_client_available_margin(self) -> float:
        """
        Get available margin for the client.

        Returns:
            float: The available margin for the client.
        """

        if _is_neo_client(self.client):
            margin = float(self.client.limits()["Net"])
            return _margin_after_buffer(margin)

        margin = float(self.client.margin()[0]["NetAvailableMargin"])
        return _margin_after_buffer(margin)

    def get_completed_buy_order_count(self) -> int:
        """
        Returns the number of completed buy orders for the client.

        This method checks if the client has a 'NeoWebSocket' attribute. If so, it retrieves the order report,
        filters the orders to count only those where the transaction type ('trnsTp') is 'B' (buy) and the status ('stat')
        is 'COMPLETE'. Returns the count of such orders. If the client does not have 'NeoWebSocket', returns 0.

        Returns:
            float: The number of completed buy orders.
        """
        if _is_neo_client(self.client):
            response = self.client.order_report()
            count = len(
                [
                    item
                    for item in response["data"]
                    if item.get("trnsTp") == "B"
                    and item.get("stat").upper() == "COMPLETE"
                ]
            )
            return count
        return 0
