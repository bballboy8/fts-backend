# Generated by CodiumAI

# Dependencies:
# pip install pytest-mock
import pytest

from app.models.nasdaq import fetch_all_data


class TestFetchAllData:
    # fetches all data when no filters are applied
    @pytest.mark.asyncio
    async def test_fetches_all_data_no_filters(self, mocker):
        from app.main import db_params

        mock_conn = mocker.patch("app.models.nasdaq.asyncpg.connect")
        mock_conn.return_value.fetch.return_value = []

        records = await fetch_all_data()

        mock_conn.assert_called_once_with(**db_params)
        mock_conn.return_value.fetch.assert_called_once_with(
            "SELECT * FROM stock_data where msgType = 'T'"
        )
        assert records == []

    # handles invalid datetime format for start_datetime
    @pytest.mark.asyncio
    async def test_handles_invalid_datetime_format(self, mocker):
        with pytest.raises(ValueError):
            await fetch_all_data(start_datetime="invalid-datetime")
