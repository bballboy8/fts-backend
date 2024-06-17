from pydantic import BaseModel

class Nasdaq(BaseModel):
    """
    UserLogout schema representing a user's logout request.

    Attributes:
        email (str): The username of the user.
    """
    target_date: str
    symbol : str
