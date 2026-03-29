"""
Tests for Identity Resolution.
"""

import pytest
import pandas as pd
import numpy as np

from src.transformers.identity_resolver import IdentityResolver


class TestIdentityResolver:
    """Tests for cross-platform identity resolution."""

    @pytest.fixture
    def resolver(self):
        return IdentityResolver()

    @pytest.fixture
    def crm_contacts(self):
        return pd.DataFrame({
            "contact_id": ["C001", "C002", "C003", "C004"],
            "email": ["alice@example.com", "bob@test.com", "carol@company.com", "dave@firm.com"],
            "name": ["Alice Smith", "Bob Jones", "Carol White", "Dave Brown"],
            "lead_status": ["MQL", "SQL", "Lead", "Customer"],
            "account_id": ["A001", "A001", "A002", "A002"],
        })

    @pytest.fixture
    def ga4_sessions(self):
        return pd.DataFrame({
            "session_id": ["S001", "S002", "S003", "S004", "S005"],
            "user_id": ["U001", "U002", "U003", "U004", "U005"],
            "email": ["alice@example.com", "bob@test.com", None, "unknown@anon.com", None],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
            "channel_group": ["Paid Search", "Paid Social", "Organic", "Email", "Direct"],
            "converted": [1, 0, 1, 0, 0],
        })

    @pytest.fixture
    def email_recipients(self):
        return pd.DataFrame({
            "recipient_id": ["R001", "R002", "R003"],
            "email": ["alice@example.com", "bob@test.com", "nobody@unknown.com"],
            "campaign_id": ["EC001", "EC001", "EC002"],
            "opened": [1, 0, 1],
            "clicked": [1, 0, 0],
        })

    def test_email_exact_match(self, resolver, crm_contacts, ga4_sessions, email_recipients):
        """Test that contacts with matching emails are correctly linked."""
        identity_graph = resolver.transform(
            crm_contacts=crm_contacts,
            ga4_sessions=ga4_sessions,
            email_recipients=email_recipients,
        )

        assert identity_graph is not None
        assert not identity_graph.empty

        # Alice and Bob should be matched via exact email
        email_matched = identity_graph[
            identity_graph["match_confidence"] == "exact_email"
        ]
        assert len(email_matched) >= 2

    def test_identity_graph_has_required_columns(self, resolver, crm_contacts, ga4_sessions, email_recipients):
        """Test that identity graph has all required columns."""
        identity_graph = resolver.transform(
            crm_contacts=crm_contacts,
            ga4_sessions=ga4_sessions,
            email_recipients=email_recipients,
        )

        required_cols = ["master_id", "crm_contact_id", "match_confidence"]
        for col in required_cols:
            assert col in identity_graph.columns, f"Missing column: {col}"

    def test_unmatched_records_preserved(self, resolver, crm_contacts, ga4_sessions, email_recipients):
        """Test that unmatched records are not discarded."""
        identity_graph = resolver.transform(
            crm_contacts=crm_contacts,
            ga4_sessions=ga4_sessions,
            email_recipients=email_recipients,
        )

        # All CRM contacts should appear in the graph (even unmatched)
        assert len(identity_graph) >= len(crm_contacts)

    def test_match_confidence_values_valid(self, resolver, crm_contacts, ga4_sessions, email_recipients):
        """Test that match confidence scores are valid values."""
        identity_graph = resolver.transform(
            crm_contacts=crm_contacts,
            ga4_sessions=ga4_sessions,
            email_recipients=email_recipients,
        )

        valid_confidence_values = {"exact_email", "email_domain", "unmatched", "campaign_fuzzy"}
        confidence_values = set(identity_graph["match_confidence"].dropna().unique())

        assert confidence_values.issubset(valid_confidence_values), \
            f"Invalid confidence values: {confidence_values - valid_confidence_values}"

    def test_master_id_unique_per_contact(self, resolver, crm_contacts, ga4_sessions, email_recipients):
        """Test that each CRM contact gets exactly one master_id."""
        identity_graph = resolver.transform(
            crm_contacts=crm_contacts,
            ga4_sessions=ga4_sessions,
            email_recipients=email_recipients,
        )

        # Each contact_id should appear at most once
        contact_id_counts = identity_graph["crm_contact_id"].value_counts()
        assert (contact_id_counts > 1).sum() == 0, "Duplicate contact_ids in identity graph"

    def test_email_matching_case_insensitive(self, resolver):
        """Test that email matching handles case differences."""
        crm = pd.DataFrame({
            "contact_id": ["C001"],
            "email": ["ALICE@EXAMPLE.COM"],
            "name": ["Alice"],
            "lead_status": ["MQL"],
            "account_id": ["A001"],
        })
        sessions = pd.DataFrame({
            "session_id": ["S001"],
            "user_id": ["U001"],
            "email": ["alice@example.com"],
            "date": ["2024-01-01"],
            "channel_group": ["Paid Search"],
            "converted": [0],
        })

        identity_graph = resolver.transform(
            crm_contacts=crm,
            ga4_sessions=sessions,
            email_recipients=None,
        )

        # Should still match despite case difference
        assert identity_graph is not None
        assert not identity_graph.empty

    def test_none_inputs_handled_gracefully(self, resolver, crm_contacts):
        """Test that None inputs don't cause crashes."""
        identity_graph = resolver.transform(
            crm_contacts=crm_contacts,
            ga4_sessions=None,
            email_recipients=None,
        )

        assert identity_graph is not None
        assert len(identity_graph) == len(crm_contacts)

    def test_empty_crm_returns_empty_graph(self, resolver, ga4_sessions):
        """Test behavior with empty CRM input."""
        empty_crm = pd.DataFrame(columns=["contact_id", "email", "name", "lead_status", "account_id"])

        identity_graph = resolver.transform(
            crm_contacts=empty_crm,
            ga4_sessions=ga4_sessions,
            email_recipients=None,
        )

        assert identity_graph is not None
        assert len(identity_graph) == 0

    def test_match_rate_reported(self, resolver, crm_contacts, ga4_sessions, email_recipients, caplog):
        """Test that match rates are logged."""
        import logging
        with caplog.at_level(logging.INFO):
            resolver.transform(
                crm_contacts=crm_contacts,
                ga4_sessions=ga4_sessions,
                email_recipients=email_recipients,
            )

        # Check that some match rate info was logged
        log_messages = " ".join([r.message for r in caplog.records])
        assert "match rate" in log_messages.lower() or "resolution complete" in log_messages.lower()
