from sqlalchemy import create_engine, Column, String, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Company(Base):
    __tablename__ = 'company'
    id = Column(UUID(as_uuid=True), primary_key=True)
    domain = Column(String(255), nullable=False)
    company_name = Column(String(255), nullable=False)

class Person(Base):
    __tablename__ = 'person'
    id = Column(UUID(as_uuid=True), primary_key=True)
    first_name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    linkedin_id = Column(String(255))
    linkedin_url = Column(String(255))
    personal_email = Column(String(255))
    location = Column(String(255))
    country = Column(String(255))
    company_id = Column(UUID(as_uuid=True), ForeignKey('company.id'), nullable=False)
    professional_email = Column(String(255))
    mobile_phone = Column(String(20))
    title = Column(String(255), nullable=False)
    seniority = Column(String(255))
    department = Column(String(255))
    quality = Column(String(50))
    email_verified = Column(Boolean)
    email_verified_status = Column(String(50))
    company = relationship("Company")
