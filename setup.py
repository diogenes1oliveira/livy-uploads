# -*- encoding: utf-8 -*-
# Source: https://packaging.python.org/guides/distributing-packages-using-setuptools/

import io
import re

from setuptools import find_packages, setup

with io.open("./livy_uploads/__init__.py", encoding="utf8") as version_f:
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]", version_f.read(), re.M
    )
    if version_match:
        version = version_match.group(1)
    else:
        raise RuntimeError("Unable to find version string.")


def read_reqs(path):
    with io.open(path, encoding="utf8") as fp:
        return [
            line.strip()
            for line in fp
            if line.strip() and not line.strip().startswith("#")
        ]


run_requirements = read_reqs("./requirements.txt")
dev_requirements = read_reqs("./requirements-dev.txt")
magics_requirements = read_reqs("./requirements-magics.txt")

with io.open("README.md", encoding="utf8") as readme:
    long_description = readme.read()

setup(
    name="livy-uploads",
    version=version,
    author="Diógenes Oliveira",
    author_email="diogenes1oliveira@gmail.com",
    packages=find_packages(
        include=["livy_uploads", "livy_uploads.*"]
    ),
    include_package_data=True,
    url="https://github.com/diogenes1oliveira/livy-uploads",
    license="MIT",
    description="Upload files and arbitrary objects to Livy",
    long_description=long_description,
    long_description_content_type='text/markdown',
    zip_safe=False,
    install_requires=run_requirements,
    extras_require={
        "dev": dev_requirements,
        "unit": dev_requirements,
        "integration": dev_requirements,
        "magics": magics_requirements,
    },
    python_requires=">=3.6",
    classifiers=[
        "Intended Audience :: Information Technology",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6",
    ],
    keywords=[],
)
