"""
setup.py - lang2sql 패키지의 설치 및 배포 구성을 정의하는 파일입니다.

이 파일은 setuptools를 사용하여 패키지 메타데이터, 의존성, CLI 엔트리 포인트 등을 지정하며,
pip 또는 Python 배포 도구들이 이 정보를 바탕으로 설치를 수행합니다.
"""

import os
import glob
from setuptools import find_packages, setup

from version import __version__


def load_requirements(path="requirements.txt"):
    """
    주어진 경로의 requirements.txt 파일을 읽어 의존성 목록을 반환합니다.

    각 줄을 읽고, 빈 줄이나 주석(#)은 무시합니다.

    Args:
        path (str): 읽을 requirements 파일 경로 (기본값: 'requirements.txt')

    Returns:
        list[str]: 설치할 패키지 목록
    """

    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


requirements = load_requirements()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="lang2sql",
    version=__version__,
    author="ehddnr301",
    author_email="dy95032@gmail.com",
    url="https://github.com/CausalInferenceLab/Lang2SQL",
    description="Lang2SQL - Query Generator for Data Warehouse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages() + ["prompt"],
    include_package_data=False,
    package_data={
        "prompt": ["*.md"],
    },
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "lang2sql = cli.__init__:cli",
        ],
    },
)
