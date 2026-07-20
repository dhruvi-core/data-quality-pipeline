from pydantic import BaseModel
from typing import Optional


class ValidationRuleCreate(BaseModel):

    dataset: str

    column_name: str

    column_type: str

    operator: str

    value: Optional[str] = None