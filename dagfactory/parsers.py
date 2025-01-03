import ast


class SafeEvalVisitor(ast.NodeVisitor):
    def __init__(self, dataset_map):
        self.dataset_map = dataset_map

    def evaluate(self, tree):
        return self.visit(tree)

    def visit_Expression(self, node):
        return self.visit(node.body)

    def visit_BinOp(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(node.op, ast.BitAnd):
            return left & right
        elif isinstance(node.op, ast.BitOr):
            return left | right
        else:
            raise ValueError(f"Unsupported binary operation: {type(node.op).__name__}")

    def visit_Name(self, node):
        if node.id in self.dataset_map:
            return self.dataset_map[node.id]
        raise NameError(f"Undefined variable: {node.id}")

    def visit_Constant(self, node):
        return node.value

    def generic_visit(self, node):
        raise ValueError(f"Unsupported syntax: {type(node).__name__}")
