import importlib.util
import logging
from pathlib import Path
import shutil
from typing import List, Optional, Union
import zipfile


LOGGER = logging.getLogger(__name__)


def pack_package(package: str, output_dir: Union[str, Path], globs: Optional[List[str]] = None) -> Path:
    '''
    Packs a Python package into a ZIP file.

    Args:
        package: The top-level package name to be packed. Must not contain dots.
        output_dir: The path to the output directory.
        globs: The glob patterns to use to find the package files. Defaults to ['*.py'].

    Returns:
        Path to the created ZIP file
    '''
    if globs is None:
        globs = ['*.py']

    if '.' in package:
        raise ValueError('package must be a single module')

    output_dir = Path(output_dir).absolute()
    if output_dir.is_file():
        raise ValueError('output_dir must be a directory')

    output_path = output_dir / f'{package}.zip'
    spec = importlib.util.find_spec(package)
    if not spec:
        raise FileNotFoundError(f'package {package} not found')

    origin = Path(spec.origin or '')
    if not spec.origin or not origin.exists():
        raise IOError(f'package {package} has no file origin')

    tmp_path = output_path.parent / f'.{output_path.name}.tmp'

    if origin.name.endswith('.zip'):
        LOGGER.info('using ZIP package at %s', origin)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(origin, tmp_path)
    else:
        with zipfile.ZipFile(tmp_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            if origin.name == '__init__.py':
                LOGGER.info('recursively adding package directory at %s', origin.parent)
                for glob_pattern in globs:
                    for path in origin.parent.rglob(glob_pattern):
                        if path.is_file():
                            zipf.write(path, f'{package}/{path.relative_to(origin.parent)}')
            else:
                LOGGER.info('adding single package file at %s', origin)
                zipf.write(origin, f'{package}.py')

    shutil.move(tmp_path, output_path)
    LOGGER.info('ZIP package saved to %s', output_path)
    return output_path

