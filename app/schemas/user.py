from pydantic import BaseModel, EmailStr, Field
from typing import Optional


class TradingExperience(BaseModel):
    """
    Attributes:
        Quetions
    """

    question_1: str = Field(..., title="Is the company in the securities industry?")
    question_2: str = Field(
        ..., title="Which of the following data products do you currently use?"
    )
    question_3: str = Field(
        ...,
        title="Did you choose the data products you subscribe to or are they provided by default by your brokerage account, software vendor or employer?",
    )
    question_4: str = Field(
        ...,
        title="Are you willing to be contacted by email concerning these and other new data products?",
    )
    question_5: str = Field(
        ...,
        title="Are you a subscriber or independent contractor, which are considered professionals and deemed to be extensions of the firm rather than natural persons?",
    )
    question_6: str = Field(
        ...,
        title="Are you registered with any state, federal or international securities agency or self-regulatory body?",
    )
    question_7: str = Field(..., title="Are you engaged as an Investment Advisor?")
    question_8: str = Field(
        ...,
        title="Are you employed by an organization that is exempt from U.S. securities laws that would otherwise require registration?",
    )
    question_9: str = Field(
        ...,
        title="Are you using or planning to use NASDAQ data for any reason other than personal use?",
    )


class UserSignUp(BaseModel):
    """
    UserSignUp schema representing user registration details.

    Attributes:
        user_id (str): The unique identifier for the user.
        password (str): The user's password.
        first_name (str): The user's first name.
        last_name (str): The user's last name
        email (EmailStr): The user's email address.
        company_name (Optional[str]): The user's company name.
        phone (Optional[str]): The user's phone number
        address_1 (Optional[str]): The user's primary address.
        address_2 (Optional[str]): The user's secondary adgdress.
        city (Optional[str]): The user's city.
        state (Optional[str]): The user's state
        region (Optional[str]): The user's region.
        postal_code (Optional[str]): The user's postal code.
        country (Optional[str]): The user's country.
        trading_experience (TradingExperience): The user's trading experience details
    """

    user_id: str = Field(..., title="User ID", max_length=50)
    password: str = Field(..., title="Password", min_length=8)
    first_name: str = Field(..., title="First Name", max_length=50)
    last_name: str = Field(..., title="Last Name", max_length=50)
    email: EmailStr = Field(..., title="Email")
    company_name: Optional[str] = Field(None, title="Company Name", max_length=100)
    phone: Optional[str] = Field(None, title="Phone", max_length=20)
    address_1: Optional[str] = Field(None, title="Address 1", max_length=100)
    address_2: Optional[str] = Field(None, title="Address 2", max_length=100)
    city: Optional[str] = Field(None, title="City", max_length=50)
    state: Optional[str] = Field(None, title="State", max_length=50)
    region: Optional[str] = Field(None, title="Region", max_length=50)
    postal_code: Optional[str] = Field(None, title="Postal Code", max_length=20)
    country: Optional[str] = Field(None, title="Country", max_length=50)
    trading_experience: TradingExperience = Field(..., title="Trading Experience")


class BulkUser(BaseModel):
    """
    UserSignUp schema representing user registration details.

    Attributes:
        user_id (str): The unique identifier for the user.
        hashed_password (str): The user's password hashed.
        first_name (str): The user's first name.
        last_name (str): The user's last name
        email (EmailStr): The user's email address.
        company_name (Optional[str]): The user's company name.
        phone (Optional[str]): The user's phone number
        address_1 (Optional[str]): The user's primary address.
        address_2 (Optional[str]): The user's secondary adgdress.
        city (Optional[str]): The user's city.
        state (Optional[str]): The user's state
        region (Optional[str]): The user's region.
        postal_code (Optional[str]): The user's postal code.
        country (Optional[str]): The user's country.
        trading_experience (TradingExperience): The user's trading experience details
    """

    user_id: str = Field(..., title="User ID", max_length=50)
    hashed_password: str = Field(..., title="Password", min_length=8)
    first_name: str = Field(..., title="First Name", max_length=50)
    last_name: str = Field(..., title="Last Name", max_length=50)
    email: EmailStr = Field(..., title="Email")
    company_name: Optional[str] = Field(None, title="Company Name", max_length=100)
    phone: Optional[str] = Field(None, title="Phone", max_length=20)
    address_1: Optional[str] = Field(None, title="Address 1", max_length=100)
    address_2: Optional[str] = Field(None, title="Address 2", max_length=100)
    city: Optional[str] = Field(None, title="City", max_length=50)
    state: Optional[str] = Field(None, title="State", max_length=50)
    region: Optional[str] = Field(None, title="Region", max_length=50)
    postal_code: Optional[str] = Field(None, title="Postal Code", max_length=20)
    country: Optional[str] = Field(None, title="Country", max_length=50)
    trading_experience: TradingExperience = Field(..., title="Trading Experience")


# UserSettings and related schemas
class ChartLayout(BaseModel):
    """
    ChartLayout schema representing a chart layout configuration.

    Attributes:
        layout_name (str): The name of the layout.
        chart_count (int): The number of charts in the layout.
    """

    layout_name: str = Field(..., description="Name of the layout")
    chart_count: int = Field(..., description="Number of charts in the layout")


class LoadLayout(BaseModel):
    """
    LoadLayout schema representing the configuration for loading a layout.

    Attributes:
        layout_name (str): The name of the layout to load.
    """

    layout_name: str = Field(..., description="Name of the layout to load")


class UserSettings(BaseModel):
    """
    UserSettings schema representing user preferences and settings.

    Attributes:
        theme (Optional[str]): The theme preference (light or dark).
        notifications (bool): Whether notifications are enabled or disabled.
        language (Optional[str]): The preferred language for the user interface
        saved_layouts (Optional[List[ChartLayout]]): A list of saved chart layouts
        active_layout (Optional[ChartLayout]): The currently active chart layout
        save_layout (Optional[SaveLayout]): The configuration for saving a layout
        load_layout (Optional[LoadLayout]): The configuration for loading a layout
    """

    theme: Optional[str] = Field("light", description="Theme preference: light or dark")
    notifications: bool = Field(True, description="Enable or disable notifications")
    language: Optional[str] = Field("en", description="Preferred language for UI")

    active_layout: Optional[ChartLayout] = Field(
        None, description="Currently active chart layout"
    )
    load_layout: Optional[LoadLayout] = Field(
        None, description="Load layout configuration"
    )


class UpdateUserSettingsRequest(BaseModel):
    """
    UpdateUserSettingsRequest schema representing a request to update user settings.

    Attributes:
        email (str): The username of the user.
        settings (UserSettings): The user's updated settings.
    """

    email: str
    settings: UserSettings


class UserLogin(BaseModel):
    """
    UserLogin schema representing a user's login credentials.

    Attributes:
        email (str): The username of the user.
        password (str): The user's password.
    """

    email: str
    password: str


class UserLogout(BaseModel):
    """
    UserLogout schema representing a user's logout request.

    Attributes:
        email (str): The username of the user.
    """

    email: str
