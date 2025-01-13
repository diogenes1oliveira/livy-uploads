from pathlib import Path

import papermill


def test_example_magics(request):
    rootdir = request.config.rootdir

    input_path = Path(rootdir) / "examples" / "magics.ipynb"
    output_path = Path(rootdir) / "tmp" / "output.ipynb"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    papermill.execute_notebook(
        input_path=str(input_path),
        output_path=str(output_path),
        cwd=str(input_path.parent),
        progress_bar=False,
    )
