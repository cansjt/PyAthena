# -*- coding: utf-8; -*-
import re

import pytest
from sqlalchemy import Column, func, Integer, MetaData, select, String, Table
from sqlalchemy.sql.ddl import CreateTable
from tests.conftest import SCHEMA

from pyathena.sqlalchemy_athena import AthenaDialect


@pytest.fixture
def expected_location(location, schema_name, table_name):
    if location[-1] != '/':
        location += '/'
    if schema_name:
        return f'{location}{schema_name}/{table_name}/'
    return f'{location}{table_name}/'


@pytest.fixture(params=(
    pytest.param('s3://my-bucket/', id='bucket w. trailing /'),
))
def location(request):
    return request.param


@pytest.fixture(params=(
    pytest.param(SCHEMA, id=f'Schema "{SCHEMA}"'),
))
def schema_name(request):
    return request.param


@pytest.fixture(params=(
    pytest.param('table_name', id='some table name'),
))
def table_name(request):
    return request.param


@pytest.mark.parametrize(('location', ), (
    pytest.param('s3://my-bucket', id='bucket no trailing /'),
    pytest.param('s3://my-bucket/', id='bucket w. trailing /'),
    pytest.param('s3://my-bucket/some/prefix', id='bucket with prefix'),
), indirect=True)
@pytest.mark.parametrize(('schema_name', ), (
    pytest.param(None, id='no schema'),
    pytest.param(SCHEMA, id=f'Schema "{SCHEMA}"'),
), indirect=True)
def test_create_table(expected_location, location, schema_name, table_name):
    """Test to expose issue #258"""
    # Given
    table = Table(table_name,
                  MetaData(),
                  Column('column_name', String),
                  schema=schema_name,
                  awsathena_location=location,
                  )
    dialect = AthenaDialect()
    location_pattern = re.compile(r"(?:LOCATION ')([^']+)(?:')")

    # When
    statement = CreateTable(table).compile(dialect=dialect)

    # Then
    assert statement is not None
    assert location_pattern.findall(str(statement))[0] == expected_location


def test_create_table_with_int_column(location, schema_name, table_name):
    """Test to expose issue #260 """
    # Given
    table = Table(table_name,
                  MetaData(),
                  Column('column_name', Integer),
                  schema=schema_name,
                  awsathena_location=location,
                  )
    dialect = AthenaDialect()
    column_pattern = re.compile(r"(?:column_name )([^ ,)\n]+)(?:[,) \n])")

    # When
    statement = CreateTable(table).compile(dialect=dialect)

    # Then
    assert column_pattern.findall(str(statement))[0] == 'INT'


def test_ddl_int_compilation_fix_does_not_break_dml():
    """Ensure the fix for issue #260 does not introduce regressions in DML compilation
    """
    # Given
    query = select(func.cast('1234', Integer))
    dialect = AthenaDialect()

    # When
    statement = dialect.statement_compiler(dialect, query)

    # Then
    assert "INTEGER" in str(statement)


# vim: et:sw=4:syntax=python:ts=4:
