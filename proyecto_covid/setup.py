from setuptools import find_packages, setup

setup(
    name="proyecto_covid",
    packages=find_packages(exclude=["proyecto_covid_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
