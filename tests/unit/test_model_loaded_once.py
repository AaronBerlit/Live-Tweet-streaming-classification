"""C12: the model is loaded once, at stream startup -- never per batch.

A reload inside `foreachBatch` or the progress listener would not break
anything visibly; it would just add seconds to every micro-batch and quietly
turn the latency figures (C22) into a measurement of model loading. So this is
checked structurally, on the source, where the mistake would be made: there is
exactly one `PipelineModel.load` call in the streaming job, it lives in
`main()`, and it is not inside any loop.

At runtime the same fact is visible in the stream's log as
"model loaded once at startup in N s".
"""

from __future__ import annotations

import ast
from pathlib import Path

SOURCE = Path(__file__).resolve().parents[2] / "pipeline" / "stream_job.py"


def _load_calls(tree: ast.AST) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "load"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "PipelineModel"
    ]


def _enclosing(tree: ast.AST, target: ast.AST) -> list[ast.AST]:
    """Chain of nodes from the module down to `target`."""
    path: list[ast.AST] = []

    def visit(node: ast.AST, trail: list[ast.AST]) -> bool:
        if node is target:
            path.extend(trail)
            return True
        return any(visit(child, trail + [node]) for child in ast.iter_child_nodes(node))

    visit(tree, [])
    return path


def test_exactly_one_model_load_in_the_stream_job():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    assert len(_load_calls(tree)) == 1


def test_the_load_is_in_main_and_not_in_any_loop_or_class():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    (call,) = _load_calls(tree)
    chain = _enclosing(tree, call)

    functions = [n.name for n in chain if isinstance(n, ast.FunctionDef)]
    assert functions == ["main"], f"PipelineModel.load is inside {functions}"
    assert not any(isinstance(n, ast.ClassDef) for n in chain), (
        "PipelineModel.load is inside a class -- the batch writer or the "
        "listener would reload it"
    )
    assert not any(isinstance(n, (ast.For, ast.While, ast.AsyncFor)) for n in chain), (
        "PipelineModel.load is inside a loop"
    )


def test_the_batch_writer_and_listener_never_touch_the_model():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    per_batch = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name in {"BatchWriter", "ProgressListener"}
    ]
    assert {c.name for c in per_batch} == {"BatchWriter", "ProgressListener"}
    for cls in per_batch:
        names = {n.id for n in ast.walk(cls) if isinstance(n, ast.Name)}
        assert "PipelineModel" not in names, f"{cls.name} references the model"
