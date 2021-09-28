"""The job database table."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, DateTime, Enum, Index, Integer, String, Text
from sqlalchemy.orm import relationship

from ..models import ErrorType, ExecutionPhase
from .base import Base

if TYPE_CHECKING:
    from datetime import datetime
    from typing import List, Optional

    from .job_parameter import JobParameter
    from .job_result import JobResult

__all__ = ["Job"]


class Job(Base):
    __tablename__ = "job"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    message_id: Optional[str] = Column(String(64))
    owner: str = Column(String(64), nullable=False)
    phase: ExecutionPhase = Column(Enum(ExecutionPhase), nullable=False)
    run_id: Optional[str] = Column(String(64))
    creation_time: datetime = Column(DateTime, nullable=False)
    start_time: Optional[datetime] = Column(DateTime)
    end_time: Optional[datetime] = Column(DateTime)
    destruction_time: datetime = Column(DateTime, nullable=False)
    execution_duration: int = Column(Integer, nullable=False)
    quote: Optional[datetime] = Column(DateTime)
    error_type: Optional[ErrorType] = Column(Enum(ErrorType))
    error_message: Optional[str] = Column(Text)
    error_detail: Optional[str] = Column(Text)

    parameters: List[JobParameter] = relationship(
        "JobParameter", cascade="delete", lazy="selectin", uselist=True
    )
    results: List[JobResult] = relationship(
        "JobResult", cascade="delete", lazy="selectin", uselist=True
    )

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        Index("by_owner_phase", "owner", "phase", "creation_time"),
        Index("by_owner_time", "owner", "creation_time"),
    )
