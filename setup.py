import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="django-commcare",
    version="0.1",
    description="CommCare integration for Django applications",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/david-pettifor-nd/django-commcare",
    author="David W Pettifor",=
    license="GPL",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=["django_commcare"],
    include_package_data=True,
    install_requires=["xlsxwriter"],
    # entry_points={
    #     "console_scripts": [
    #         "realpython=reader.__main__:main",
    #     ]
    # },
)
