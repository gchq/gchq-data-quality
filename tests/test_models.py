# (c) Crown Copyright GCHQ \n
import pytest

from gchq_data_quality.models import DamaFramework


def test_dama_framework_accepts_known_values() -> None:
    # Expected to work
    assert DamaFramework("Uniqueness") == DamaFramework.Uniqueness
    assert DamaFramework("uniqueness") == DamaFramework.Uniqueness
    assert DamaFramework("VALIDITY") == DamaFramework.Validity
    assert DamaFramework("timeliness") == DamaFramework.Timeliness


def test_dama_framework_raises_on_unknown_value() -> None:
    with pytest.raises(ValueError, match="is not a valid"):
        DamaFramework("NonExistent")
