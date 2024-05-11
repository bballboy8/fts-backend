from pydantic import BaseModel, EmailStr, Field, validator
from typing import Optional, Dict, List

class User(BaseModel):
    username: str
    email: EmailStr

# class UserSignUp(BaseModel):
#     first_name: str
#     last_name: str
#     email: EmailStr
#     password: str
#     confirm_password: str

class TradingExperience(BaseModel):
    question_1: str = Field(..., title="This is Question 1")
    question_2: str = Field(..., title="This is Question 2")
    question_3: str = Field(..., title="This is Question 3")
    question_4: str = Field(..., title="This is Question 4")
    question_5: str = Field(..., title="This is Question 5")

class UserSignUp(BaseModel):
    first_name: str = Field(..., title="First Name", max_length=50)
    last_name: str = Field(..., title="Last Name", max_length=50)
    username: EmailStr = Field(..., title="Email")
    password: str = Field(..., title="Password", min_length=8)
    confirm_password: str = Field(..., title="Confirm Password", min_length=8)
    trading_experience: TradingExperience = Field(..., title="Trading Experience")

    @validator('confirm_password')
    def passwords_match(cls, v, values, **kwargs):
        if 'password' in values and v != values['password']:
            raise ValueError('Passwords do not match')
        return v

# Task 2 - Implement the UserSettings schema with to right info from figma
class ChartLayout(BaseModel):
    layout_name: str = Field(..., description="name of layout")
    chart_count: int = Field(..., description="Number of charts in the layout")

class SaveLayout(BaseModel):
    layout_name: str = Field(..., description="name of layout to save")
    chart_count: int = Field(..., description="Number of charts to save in the layout")

class LoadLayout(BaseModel):
    layout_name: str = Field(..., description="name of layout to load")
    
    
class UserSettings(BaseModel):
    theme: Optional[str] = Field("light", description="Theme preference: light or dark")
    notifications: bool = Field(True, description="Enable or disable notifications")
    language: Optional[str] = Field("en", description="Preferred language for UI")
    saved_layouts: Optional[List[ChartLayout]] = Field(
        None, description="List of saved chart layouts"
    )
    
    active_layout: Optional[ChartLayout] = Field(
        None, description="Currently active chart layout"
    )
    
    save_layout: Optional[SaveLayout] = Field(
        None, description="Save layout"
    )
    
    load_layout: Optional[LoadLayout] = Field(
        None, description="Load layout"
    )
    
# use it like this
# example_settings = UserSettings(
#     theme="dark",
#     notifications=True,
#     language="en",
#     saved_layouts=[
#         ChartLayout(layout_name="Default", chart_count=1),
#         ChartLayout(layout_name="Multi-chart", chart_count=4)
#     ],
#     active_layout=ChartLayout(layout_name="Default", chart_count=1),
#     save_layout=SaveLayout(layout_name="New Layout", chart_count=6),
#     load_layout=LoadLayout(layout_name="Multi-chart")
# )
    

class UpdateUserSettingsRequest(BaseModel):
    username: str
    settings: UserSettings

class UserLogin(BaseModel):
    username: str
    password: str

class UserLogout(BaseModel):
    username: str
