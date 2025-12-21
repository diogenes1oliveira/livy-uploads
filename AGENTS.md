# Agent Instructions for livy-uploads Project

## General Guidelines

1. **Keep it simple**: Only implement what is explicitly requested
2. **Don't over-engineer**: Avoid creating complex abstractions unless specifically asked
3. **Stay focused**: Don't add extra features, integrations, or "improvements" beyond the request
4. **Ask before expanding**: If you think something might be useful, ask first rather than implementing it
5. **Don't get stuck in a loop**: If you have to do too much back and forth changing and experimenting, stop and ask me.
   Likely there's some architectural problem.

## Coding Preferences

- IMPORTANT: whatever you need to run some Python-based command, always prefix with `uv run`: `uv run ruff`, `uv run mypy`, etc.
- To check out unused imports, use ruff;
- Format your code with black;
- Prefer simple, direct implementations over complex class hierarchies
- Don't create new files unless explicitly requested
- Don't modify existing files beyond what's needed for the specific request
- Don't add extensive documentation, error handling, or configuration unless asked
- Prefer the full form from importing modules; only use the relative .submodule in the `__init__.py`
- Import the `dataclasses` module and decorate with `@dataclasses.dataclass`;
- When using click.echo, you should always echo to stderr unless it's very explicit the output should go to stdout.
- If a types stub is available, you should always try to install it;
- No need for re-exporting stuff in the `__init__.py`;
- When adding tests, mirror the main package submodule structure. Also, always add the intermediate `__init__.py`;
- Always add type annotations. Only exception are the test methods and fixtures: the return value don't need type
  annotations, but the arguments do.
- **Logging**: Always assign a global `LOGGER = logging.logger` at the top of each module and use it to log important modifications (creation, deletion, updates, etc). Log at INFO level for successful operations and WARNING/ERROR for failures.

## Example

**Good**: User asks for "a logging handler that calls Java gateway" → Create a simple class with an emit method

**Bad**: User asks for "a logging handler that calls Java gateway" → Create multiple files, integration functions, configuration options, comprehensive documentation, etc.

## When in doubt

Ask the user: "Should I keep this simple or would you like me to expand on it?"

## Testing

- Always use TDD: create the tests before the code.
- When creating tests, start with just the pytest skeleton, and the test methods with a single pass statement. Don't add comments unless explicitly asked to, because the test names themselves should be descriptive.
- Add the `mypy: disable-error-code=no-untyped-def`at the header of all tests. so you don't need to add -> None everywhere.
- Use the standard setup, act, verify structure in the test method code, in separate blocks. (no need to add the comments describing these names)
- Prefer using the tmp_path, monkeypatch, etc pytest fixtures over manually setting up tmp paths, import paths, etc
- When you need to setup/close soemthing or the same instance is created multiple times in the test class, always add a fixture
- **Define all fixtures at the top of the test class** before any test methods, and use them consistently across all tests
- **Avoid creating inline test classes** - use fixtures instead. If you need special behavior, configure the fixture's mocks with side_effect rather than creating new fixtures
- **Don't create additional fixtures unless absolutely necessary** - prefer configuring existing fixtures with mocks, side_effects, or parameter modifications
- **Actually run tests after writing them**: don't just say "ready for testing" - run pytest, show output, fix failures iteratively until all pass.
- **Don't try to run all the tests at once**: this package has some very slow tests, only run the tests related to the current one.

## After Implementation

- Run `uv run black` to format all modified files
- Run `uv run ruff check` and fix all warnings
- Run full test suite to verify no regressions
- Delete any old/unused files
- If there's a plan, write a comprehensive report in reports/ following the style of existing reports (objective, summary, files created/modified/deleted, technical details, validation, success criteria)
