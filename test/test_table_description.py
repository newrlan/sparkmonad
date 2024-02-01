# import pandas as pd
import pytest
# from contextlib import nullcontext as does_not_raise

from SparkMonad.table_description import TableDescription


def test_TableDescription_duplicate_columns():
    with pytest.raises(ValueError, match=r'There are duplicated columns in *'):
        TableDescription('test_tab',
                         [('a', int), ('b', str), ('a', float)]
                         )


def test_TableDescription_empty_columns():
    link = 'TEST_TAB'
    with pytest.raises(ValueError, match=f"Table description {link} desn't have any column."):
        TableDescription(link, [])


@pytest.fixture
def tables_lib():
    res = [
            TableDescription('tab_1', [('a', 'int')]),      # 0
            TableDescription('tab_2', [('a', 'double')]),   # 1
            TableDescription('tab_2', [('a', 'int')]),      # 2
            TableDescription('tab_3', [('a', 'int'), ('b', 'string')]),    # 3
            TableDescription('tab_4', [('a', 'int'), ('c', 'string')]),    # 4
            TableDescription('tab_4', [('a', 'int'), ('b', 'string')]),    # 5
            TableDescription('tab_3', [('a', 'string'), ('b', 'int')]),    # 6
            TableDescription('tab_1', [('a', 'int')]),      # 7
            TableDescription('tab_3', [('a', 'int'), ('b', 'string')]),    # 8
            ]
    return res


@pytest.mark.parametrize('i, j, ans', [
        (0, 0, True), (0, 7, True), (3, 4, False), (3, 5, False),
        (3, 6, False), (4, 5, False), (0, 3, False), (3, 8, True)
    ]
 )
def test_TableDescription_equality(tables_lib, i, j, ans):
    td_1 = tables_lib[i]
    td_2 = tables_lib[j]
    res = td_1 == td_2
    assert ans == res



@pytest.mark.parametrize('i, j, ans', [
        (0, 0, True), (0, 3, True), (3, 4, False), (3, 5, True), (0, 6, False)
    ])
def test_TableDescription_is_projection_of(tables_lib, i, j, ans):
    td_i = tables_lib[i]
    td_j = tables_lib[j]
    assert td_i.is_projection_of(td_j) == ans
