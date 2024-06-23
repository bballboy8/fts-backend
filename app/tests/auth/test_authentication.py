import jwt
import pytest


class TestVerifyToken:
    # valid token returns correct payload
    def test_valid_token_returns_correct_payload(self, mocker):
        from app.auth.authentication import verify_token, SECRET_KEY, ALGORITHM

        mocker.patch("app.auth.authentication.SECRET_KEY", "test_secret")
        mocker.patch("app.auth.authentication.ALGORITHM", "HS256")

        payload = {"sub": "1234567890"}
        token = jwt.encode(payload, "test_secret", algorithm="HS256")

        result = verify_token(token)
        assert result == "1234567890"

    # token with invalid signature returns None
    def test_token_with_invalid_signature_returns_none(self, mocker):
        from app.auth.authentication import verify_token, SECRET_KEY, ALGORITHM

        mocker.patch("app.auth.authentication.SECRET_KEY", "test_secret")
        mocker.patch("app.auth.authentication.ALGORITHM", "HS256")

        payload = {"sub": "1234567890"}
        token = jwt.encode(payload, "wrong_secret", algorithm="HS256")

        result = verify_token(token)
        assert result is None
