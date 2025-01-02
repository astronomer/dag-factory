import ast
import pytest
from dagfactory.parsers import SafeEvalVisitor

@pytest.fixture
def dataset_map():
    return {
        'dataset_custom_1': 1,
        'dataset_custom_2': 2,
        'dataset_custom_3': 3
    }

@pytest.fixture
def visitor(dataset_map):
    return SafeEvalVisitor(dataset_map)

def test_evaluate(visitor):
    condition_string = "dataset_custom_1 & dataset_custom_2 | dataset_custom_3"
    tree = ast.parse(condition_string, mode='eval')
    result = visitor.evaluate(tree)
    expected = (1 & 2) | 3
    assert result == expected

def test_visit_BinOp_and(visitor):
    condition_string = "dataset_custom_1 & dataset_custom_2"
    tree = ast.parse(condition_string, mode='eval')
    result = visitor.evaluate(tree)
    expected = 1 & 2
    assert result == expected

def test_visit_BinOp_or(visitor):
    condition_string = "dataset_custom_1 | dataset_custom_3"
    tree = ast.parse(condition_string, mode='eval')
    result = visitor.evaluate(tree)
    expected = 1 | 3
    assert result == expected

def test_visit_UnaryOp_not(visitor):
    condition_string = "~dataset_custom_1"
    tree = ast.parse(condition_string, mode='eval')
    result = visitor.evaluate(tree)
    expected = ~1
    assert result == expected

def test_visit_Name(visitor):
    condition_string = "dataset_custom_2"
    tree = ast.parse(condition_string, mode='eval')
    result = visitor.evaluate(tree)
    expected = 2
    assert result == expected

def test_visit_Constant(visitor):
    condition_string = "42"
    tree = ast.parse(condition_string, mode='eval')
    result = visitor.evaluate(tree)
    expected = 42
    assert result == expected

def test_unsupported_binary_operation(visitor):
    condition_string = "dataset_custom_1 + dataset_custom_2"
    tree = ast.parse(condition_string, mode='eval')
    with pytest.raises(ValueError):
        visitor.evaluate(tree)

def test_unsupported_unary_operation(visitor):
    condition_string = "+dataset_custom_1"
    tree = ast.parse(condition_string, mode='eval')
    with pytest.raises(ValueError):
        visitor.evaluate(tree)

def test_undefined_variable(visitor):
    condition_string = "undefined_dataset"
    tree = ast.parse(condition_string, mode='eval')
    with pytest.raises(NameError):
        visitor.evaluate(tree)

def test_unsupported_syntax(visitor):
    condition_string = "[1, 2, 3]"
    tree = ast.parse(condition_string, mode='eval')
    with pytest.raises(ValueError):
        visitor.evaluate(tree)