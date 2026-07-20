from sqlalchemy import Column, Integer, String
from database import Base


class ValidationRule(Base):

    __tablename__ = "validation_rules"

    id = Column(
        Integer,
        primary_key=True,
        index=True
    )

    dataset = Column(
        String,
        nullable=False
    )

    column_name = Column(
        String,
        nullable=False
    )

    column_type = Column(
        String,
        nullable=False
    )

    operator = Column(
        String,
        nullable=False
    )

    value = Column(
        String,
        nullable=True
    )