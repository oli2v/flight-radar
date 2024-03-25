from setuptools import setup, find_packages

setup(
    name="flight-radar",
    version="1.2.1",
    description="This module implements an ELT Pipeline from FlightRadar API in a GCP environment.",
    author="Olivier Valenduc",
    author_email="o.valenduc@gmail.com",
    packages=find_packages(),
    install_requires=[
        "FlightRadarAPI",
        "tqdm",
        "pandas",
        "google-cloud-storage",
        "google-cloud-bigquery",
    ],
)
