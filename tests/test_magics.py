# import logging
# from pathlib import Path

# import papermill
# import pytest

# from livy_uploads.configs.envfile import envfile_load
# from livy_uploads.configs.uv_kernels import get_kernel_name

# # mypy: disable-error-code="no-untyped-def,import-untyped"


# LOGGER = logging.getLogger(__name__)


# @pytest.mark.slow
# def test_example_magics(request: pytest.FixtureRequest) -> None:
#     rootdir = Path(request.config.rootdir)  # type: ignore

#     input_path = rootdir / "examples" / "magics.ipynb"
#     output_path = rootdir / "var" / "output.ipynb"
#     _, env_values = envfile_load()
#     kernel_name = get_kernel_name(env_values["UV_KERNEL_NAME_PYSPARK"])

#     LOGGER.info("running example magics notebook from %s to %s (using kernel %s)", input_path, output_path, kernel_name)
#     output_path.parent.mkdir(parents=True, exist_ok=True)

#     papermill.execute_notebook(
#         input_path=str(input_path),
#         output_path=str(output_path),
#         cwd=str(input_path.parent),
#         progress_bar=False,
#         kernel_name=kernel_name,
#     )
