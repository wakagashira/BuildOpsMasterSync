"""
QuotesEntity (no-op)

Quotes are not implemented yet.
This entity exists to satisfy the engine contract and allow
end-to-end testing of the Master Sync system.
"""


class QuotesEntity:
    name = "Quotes"

    def sync(self, tenant_id, since=None):
        """
        No-op sync for Quotes.

        Returns:
            (rows_fetched, rows_merged)
        """
        return 0, 0
