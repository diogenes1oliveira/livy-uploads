#!/usr/bin/env python3

"""
Script to remove all type annotations from Python files using libcst.

Courtesy of Claude Code.
"""

import libcst as cst


class TypeAnnotationRemover(cst.CSTTransformer):
    """
    CST transformer that removes type annotations from:
    1. Function parameters
    2. Function return types
    3. Variable assignments
    4. Class attribute annotations
    5. Standalone type annotations
    
    But preserves type annotations inside NamedTuple class definitions.
    """
    
    def __init__(self):
        self.in_namedtuple = False
        self.namedtuple_bases = set()
    
    def visit_ClassDef(self, node: cst.ClassDef) -> bool:
        """Check if we're entering a NamedTuple class."""
        # Check if any base class is NamedTuple
        for base in node.bases:
            if isinstance(base.value, cst.Name) and base.value.value == "NamedTuple":
                self.in_namedtuple = True
                self.namedtuple_bases.add(node.name.value)
                break
        return True
    
    def leave_ClassDef(self, original_node: cst.ClassDef, updated_node: cst.ClassDef) -> cst.ClassDef:
        """Reset NamedTuple flag when leaving the class."""
        if original_node.name.value in self.namedtuple_bases:
            self.in_namedtuple = False
            self.namedtuple_bases.remove(original_node.name.value)
        return updated_node
    
    def leave_Param(self, original_node: cst.Param, updated_node: cst.Param) -> cst.Param:
        """Remove type annotations from function parameters."""
        return updated_node.with_changes(annotation=None)
    
    def leave_FunctionDef(self, original_node: cst.FunctionDef, updated_node: cst.FunctionDef) -> cst.FunctionDef:
        """Remove return type annotations from function definitions."""
        return updated_node.with_changes(returns=None)
    
    def leave_AnnAssign(self, original_node: cst.AnnAssign, updated_node: cst.AnnAssign) -> cst.MaybeSentinel:
        """Convert annotated assignments to regular assignments, but preserve NamedTuple fields."""
        if self.in_namedtuple:
            # Inside NamedTuple, preserve the annotated assignment
            return updated_node
        
        if updated_node.value is not None:
            # Has a value, convert to regular assignment
            return cst.Assign(
                targets=[cst.AssignTarget(target=updated_node.target)],
                value=updated_node.value
            )
        else:
            # No value, just remove the annotation
            return cst.RemoveFromParent()
    
    def leave_Import(self, original_node: cst.Import, updated_node: cst.Import) -> cst.Import:
        """Keep imports as-is."""
        return updated_node
    
    def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.ImportFrom:
        """Keep import from statements as-is."""
        return updated_node


def remove_type_annotations(source_code: str) -> str:
    """
    Remove all type annotations from the given source code.
    
    Args:
        source_code: The Python source code as a string
        
    Returns:
        The source code with all type annotations removed
    """
    module = cst.parse_module(source_code)
    
    transformer = TypeAnnotationRemover()
    modified_module = module.visit(transformer)

    return modified_module.code


if __name__ == '__main__':
    import sys

    args = sys.argv[1:]
    if args and args[0] != '-':
        with open(args[0]) as f:
            code = f.read()
    else:
        code = sys.stdin.read()

    print(remove_type_annotations(code), end='')
