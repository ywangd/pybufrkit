import pytest

from pybufrkit.tablespreparer import prepare_wmo_tables


@pytest.mark.skip
def test_tables_preparer():
    prepare_wmo_tables(35)
    prepare_wmo_tables(36)
