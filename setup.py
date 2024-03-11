from setuptools import setup

setup(
    name="flight-radar",
    version="1.0.2",
    description="This module implements an ELT Pipeline from FlightRadar API in a GCP environment.",
    author="Olivier Valenduc",
    author_email="o.valenduc@gmail.com",
    packages=["flight_radar"],
    install_requires=[
        "FlightRadarAPI",
        "tqdm",
        "pandas",
        "google-cloud-storage",
        "google-cloud-bigquery",
    ],
)
