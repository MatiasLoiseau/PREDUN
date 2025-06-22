from setuptools import find_packages, setup

setup(
    name="predun_dagster",
    packages=find_packages(exclude=["predun_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
