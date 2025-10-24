from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Any, Dict


class Event(BaseModel):
    topic: str = Field(..., min_length=1)
    event_id: str = Field(..., min_length=1)
    timestamp: datetime
    source: str = Field(..., min_length=1)
    payload: Dict[str, Any]

    @field_validator('timestamp')
    @classmethod
    def check_timestamp_format(cls, v: int):
        if not isinstance(v, datetime):
            raise ValueError('timestamp must be a datetime object')
        return v