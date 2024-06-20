from typing import Optional
from pydantic import BaseModel


class Nasdaq(BaseModel):
    """
    UserLogout schema representing a user's logout request.

    Attributes:
        email (str): The username of the user.
    """

    start_datetime: Optional[str]
    symbol: Optional[str]
