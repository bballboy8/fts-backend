# models/example_model.py
from pydantic import BaseModel

class ExampleModel(BaseModel):
    item_id: str
    item_name: str
    item_description: str
    item_price: float
