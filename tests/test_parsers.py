import pytest
from dagfactory.parsers import PyparsingExpressionParser 


def P():
    return PyparsingExpressionParser()


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("a", "a"),
        ("dataset_1", "dataset_1"),
        ("a&b", {"and": ["a", "b"]}),
        ("a|b", {"or": ["a", "b"]}),
        ("a & b | c", {"or": [{"and": ["a", "b"]}, "c"]}),
        ("(a & b) | (c & d)", {"or": [{"and": ["a", "b"]}, {"and": ["c", "d"]}]}),
        ("a & (b | c)", {"and": ["a", {"or": ["b", "c"]}]}),
        ("s3://bucket/key_a | db.schema.table",
         {"or": ["s3://bucket/key_a", "db.schema.table"]}),
    ],
)
def test_parse_success(expr, expected):
    parser = P()
    tree = parser.parse(expr)
    assert tree == expected


@pytest.mark.parametrize("expr", ["", "a && b", "a || b", "a |", "& b", "(a & b", "a b"])
def test_parse_invalid(expr):
    parser = P()
    with pytest.raises(ValueError) as ei:
        parser.parse(expr)
    # mensagem amigável
    assert "Invalid asset expression" in str(ei.value)


def test_bucket():
    parser = P()
    tree = parser.parse("((s3://bucket-cjmm/raw/dataset_custom_1 & s3://bucket-cjmm/raw/dataset_custom_2) | s3://bucket-cjmm/raw/dataset_custom_3)")
    assert tree == {'or': [{'and': ['s3://bucket-cjmm/raw/dataset_custom_1', 's3://bucket-cjmm/raw/dataset_custom_2']}, 's3://bucket-cjmm/raw/dataset_custom_3']}


def test_leaves_are_str():
    parser = P()
    tree = parser.parse("x & y")
    assert isinstance(tree["and"][0], str)
    assert isinstance(tree["and"][1], str)


def test_traverse_with_setter_single_replace():
    parser = P()
    tree = parser.parse("a & b | c")  
    for leaf, setter in parser.traverse_with_setter(tree):
        if leaf == "b":
            setter("b2")
            break
    assert tree == {"or": [{"and": ["a", "b2"]}, "c"]}

def test_traverse_with_setter_multiple_replacements():
    parser = P()
    tree = parser.parse("(a & b) | (c & d)")
    repl = {"a": "A", "c": "C", "d": "D"}
    for leaf, setter in parser.traverse_with_setter(tree):
        if leaf in repl:
            setter(repl[leaf])
    assert tree == {"or": [{"and": ["A", "b"]}, {"and": ["C", "D"]}]}


def test_long_chain_left_associative():
    parser = P()
    tree = parser.parse("a & b & c | d")
    assert tree == {"or": [{"and": [{"and": ["a", "b"]}, "c"]}, "d"]}


def test_packrat_enabled():
    parser = P()
    expr = " & ".join([f"d{i}" for i in range(200)])
    tree = parser.parse(expr)

    def flatten(n):
        if isinstance(n, dict):
            op, kids = next(iter(n.items()))
            for k in kids:
                yield from flatten(k)
        else:
            yield n

    leaves = list(flatten(tree))
    assert len(leaves) == 200
    assert leaves[0] == "d0"
    assert leaves[-1] == "d199"

