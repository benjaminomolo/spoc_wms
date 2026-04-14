# models_master.py

import decimal
import enum
import uuid
from decimal import Decimal

import bcrypt
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey, Enum, UniqueConstraint, Boolean, Text, Date, \
    Float, Numeric, DECIMAL, Index, Time, CheckConstraint
from sqlalchemy.orm import relationship, object_session, validates, declared_attr
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event, select, func
from datetime import datetime, timezone

Base = declarative_base()


class Company(Base):
    __tablename__ = 'companies'
    id = Column(Integer, primary_key=True)
    app_name = Column(String(100), unique=True, nullable=False)
    name = Column(String(200))
    database_name = Column(String(255))  # Which database holds their data
    subdomain = Column(String(100))
    # NO relationships to other tables


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    email = Column(String(100), nullable=False)
    password = Column(String(100), nullable=False)
    name = Column(String(100))
    company_id = Column(Integer, ForeignKey('companies.id'))  # Link to company
    role = Column(String(50))
    # NO app_id needed here


class ContactMessage(Base):
    __tablename__ = 'contact_messages'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(150), nullable=False)
    message = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
