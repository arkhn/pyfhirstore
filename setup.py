import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

requirements = read("requirements.txt").split()

setuptools.setup(
    name="fhirstore",
    version="0.0.3",
    author="Arkhn",
    author_email="contact@arkhn.org",
    description="Manipulating FHIR data leveraging MongoDB as storage layer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/arkhn/pyfhirstore/",
    packages=setuptools.find_packages(),
    package_data={
        'fhirstore': ['schema/fhir.schema.json'],
    },
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)