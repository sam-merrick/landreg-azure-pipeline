"""Shared pytest configuration and fixtures."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession shared across the test session."""
    return SparkSession.builder.master("local[1]").appName("tests").getOrCreate()