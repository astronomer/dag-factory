from dataclasses import dataclass, field
from typing import  (
    List, Any, 
    Dict, TypeVar, 
    Tuple, Generator,
    Callable
)
from pyparsing import (    
    alphanums, 
    infixNotation, 
    opAssoc, 
    Word, 
    Literal, 
    ParserElement, 
    ParseResults,
    ParseException,
)

LeafSetter = Callable[[str], None]

T = TypeVar('T')

@dataclass
class PyparsingExpressionParser:
    operand: ParserElement = field(default_factory=lambda: Word(alphanums + "://_.-/"))
    AND:     ParserElement = field(default_factory=lambda: Literal("&"))
    OR:      ParserElement = field(default_factory=lambda: Literal("|"))
    _grammar: ParserElement = field(init=False, repr=False)

    def __post_init__(self) -> None:
        ParserElement.enablePackrat()
        self._grammar = infixNotation(
            self.operand,
            [
                (self.AND, 2, opAssoc.LEFT, self._make_node),
                (self.OR,  2, opAssoc.LEFT, self._make_node),
            ],
        )
    
    def _make_node(self, tokens: ParseResults)-> Dict[str, List[Any]]:
        t = tokens[0]
        left = t[0]
        i = 1
        while i < len(t):
            op = t[i]
            right = t[i + 1]
            key = "and" if op == "&" else "or"
            left = {key: [left, right]}
            i += 2
        return left


    def parse(self, expr_str: str) -> Dict[str, List[Any]]:
        try:
            parsed = self._grammar.parseString(expr_str, parseAll=True)[0]
        except ParseException as e:
            raise ValueError(f"Invalid asset expression: {e}") from e
        return self._to_dict(parsed)

    def _to_dict(self, node: Any) -> Dict[str, List[Any]]:
        if isinstance(node, dict):
            op, children = next(iter(node.items()))
            return {op: [self._to_dict(child) for child in children]}
        return str(node)
    
    def traverse_with_setter(
            self,
            tree: Dict[str, List[Any]]
    ) -> Generator[Tuple[str, LeafSetter], None, None]:
        def _rec(
            node: Dict[str, List[Any]],
            parent: List[Dict[str, List[Any]]] = None,
            idx: int = None
        ):
            if isinstance(node, dict):
                op, children = next(iter(node.items()))
                for i, child in enumerate(children):
                    yield from _rec(child, children, i)
            else:
                def set_value(new: str) -> None:
                    parent[idx] = new
                yield node, set_value
        yield from _rec(tree)