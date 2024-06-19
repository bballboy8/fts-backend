from pydantic import BaseModel

class Nasdaq(BaseModel):
    """
    UserLogout schema representing a user's logout request.

    Attributes:
        email (str): The username of the user.
    """
    target_date: str | None = None
    timestamp: int | None = None
    symbol : str | None = None
