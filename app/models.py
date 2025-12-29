from pydantic import BaseModel, Field
from datetime import datetime


class MessageIn(BaseModel):
    message_id: str
    from_: str = Field(..., alias="from")
    to: str
    ts: datetime
    text: str

    model_config = {
        "populate_by_name": True
    }


class MessageOut(BaseModel):
    message_id: str
    from_: str = Field(..., alias="from")
    to: str
    ts: datetime
    text: str

    model_config = {
        "populate_by_name": True
    }
