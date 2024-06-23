# Generated by CodiumAI

# Dependencies:
# pip install pytest-mock
import pytest

from app.models.user import save_user


class TestSaveUser:
    # Successfully saves a valid user with all required fields
    @pytest.mark.asyncio
    async def test_save_valid_user(self, mocker):
        from app.main import db_params

        # Arrange
        user_data = {
            "user_id": "123",
            "email": "test@example.com",
            "hashed_password": "hashed_password",
            "first_name": "John",
            "last_name": "Doe",
            "company_name": "Test Company",
            "phone": "1234567890",
            "address_1": "123 Test St",
            "address_2": "Apt 4",
            "city": "Test City",
            "state": "TS",
            "region": "Test Region",
            "postal_code": "12345",
            "country": "Test Country",
            "trading_experience": "5 years",
        }

        mock_conn = mocker.patch("asyncpg.connect", autospec=True)
        mock_execute = mocker.AsyncMock()
        mock_conn.return_value.execute = mock_execute

        # Act
        await save_user(user_data)

        # Assert
        mock_conn.assert_called_once_with(**db_params)
        mock_execute.assert_called_once_with(
            """
            INSERT INTO users (
                user_id, email, hashed_password, first_name, last_name,
                company_name, phone, address_1, address_2, city, state,
                region, postal_code, country, trading_experience
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            )
            """,
            user_data["user_id"],
            user_data["email"],
            user_data["hashed_password"],
            user_data["first_name"],
            user_data["last_name"],
            user_data["company_name"],
            user_data["phone"],
            user_data["address_1"],
            user_data["address_2"],
            user_data["city"],
            user_data["state"],
            user_data["region"],
            user_data["postal_code"],
            user_data["country"],
            user_data["trading_experience"],
        )
