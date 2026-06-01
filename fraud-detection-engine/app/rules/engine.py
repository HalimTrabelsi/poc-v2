"""Rule engine: SafeExpressionEvaluator and RuleEngine."""
import ast
import logging
from typing import Any

logger = logging.getLogger(__name__)


class SafeExpressionEvaluator(ast.NodeVisitor):
    """Evaluate a restricted subset of Python arithmetic/boolean expressions.

    Only whitelisted AST node types are permitted; any other node raises
    ValueError immediately, preventing code injection through rule conditions.
    """

    ALLOWED_NODES = (
        ast.Expression, ast.BoolOp, ast.BinOp, ast.UnaryOp, ast.Compare,
        ast.Name, ast.Load, ast.Constant,
        ast.And, ast.Or, ast.Not,
        ast.Eq, ast.NotEq, ast.Gt, ast.GtE, ast.Lt, ast.LtE,
        ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.Pow,
        ast.USub, ast.UAdd,
    )

    class _MissingVariable(Exception):
        """Raised when a rule references a variable that is not in the context.

        The RuleEngine catches this and treats the rule as 'not triggered',
        instead of silently defaulting the variable to 0 (which produced
        phantom triggers — e.g. TA002 fired whenever days_reg_to_first_payment
        was missing because 0 < 7).
        """

    def __init__(self, context: dict[str, Any]) -> None:
        self.context = context

    def visit(self, node: ast.AST) -> Any:  # type: ignore[override]
        if not isinstance(node, self.ALLOWED_NODES):
            raise ValueError(f"Disallowed AST node: {type(node).__name__}")
        return super().visit(node)

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id not in self.context:
            raise SafeExpressionEvaluator._MissingVariable(node.id)
        return self.context[node.id]

    def visit_BoolOp(self, node: ast.BoolOp) -> bool:
        if isinstance(node.op, ast.And):
            return all(self.visit(v) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(self.visit(v) for v in node.values)
        raise ValueError("Unsupported boolean operator")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        val = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return not val
        if isinstance(node.op, ast.USub):
            return -val
        if isinstance(node.op, ast.UAdd):
            return +val
        raise ValueError("Unsupported unary operator")

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        ops = {
            ast.Add: lambda a, b: a + b,
            ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div: lambda a, b: a / b if b != 0 else 0.0,
            ast.Mod: lambda a, b: a % b if b != 0 else 0.0,
            ast.Pow: lambda a, b: a ** b,
        }
        fn = ops.get(type(node.op))
        if fn is None:
            raise ValueError(f"Unsupported binary operator: {type(node.op).__name__}")
        return fn(left, right)

    def visit_Compare(self, node: ast.Compare) -> bool:
        left = self.visit(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            right = self.visit(comparator)
            ops = {
                ast.Eq: lambda a, b: a == b,
                ast.NotEq: lambda a, b: a != b,
                ast.Gt: lambda a, b: a > b,
                ast.GtE: lambda a, b: a >= b,
                ast.Lt: lambda a, b: a < b,
                ast.LtE: lambda a, b: a <= b,
            }
            fn = ops.get(type(op))
            if fn is None:
                raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")
            if not fn(left, right):
                return False
            left = right
        return True


class RuleEngine:
    """Evaluate a list of rule dicts against a feature context."""

    def __init__(self, rules: list[dict]) -> None:
        self._rules = rules

    def evaluate(self, features: dict) -> tuple[float, list[dict]]:
        """Return (aggregate_score, triggered_rule_dicts_with_explanations).

        Score is capped at 1.0; it is the sum of weights of triggered rules.
        """
        triggered: list[dict] = []
        total_weight = 0.0

        for rule in self._rules:
            condition = rule.get("condition", "False")
            try:
                tree = ast.parse(condition, mode="eval")
                evaluator = SafeExpressionEvaluator(features)
                matched = bool(evaluator.visit(tree))
            except SafeExpressionEvaluator._MissingVariable as exc:
                # Rule references a feature the extractor didn't provide.
                # Treat as not-triggered rather than silently defaulting to 0.
                logger.debug("Rule %s skipped (missing variable: %s)", rule.get("id"), exc)
                matched = False
            except ZeroDivisionError:
                matched = False
            except Exception as exc:
                logger.debug("Rule %s eval error: %s", rule.get("id"), exc)
                matched = False

            if matched:
                weight = float(rule.get("weight", 0.0))
                evidence_template = rule.get("evidence_template", "Rule triggered")
                try:
                    explanation = evidence_template.format(**features)
                except (KeyError, ValueError):
                    explanation = evidence_template

                triggered.append(
                    {
                        "rule_id": rule.get("id", ""),
                        "name": rule.get("name", ""),
                        "weight": weight,
                        "triggered": True,
                        "explanation": explanation,
                        "alert_level": rule.get("alert_level", "MEDIUM"),
                    }
                )
                total_weight += weight

        score = round(min(total_weight, 1.0), 4)
        return score, triggered
