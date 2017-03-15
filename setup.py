import os
import re
from setuptools import setup, find_packages
from setuptools.command.install_scripts import install_scripts


class my_install_scripts(install_scripts):
    def write_script(self, script_name, contents, mode="t", *ignored):
        contents = re.sub("import sys",
                          "import sys\nsys.path.append('/opt/graphite/lib')",
                          contents)
        install_scripts.write_script(
            self, script_name, contents, mode="t", *ignored)


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="carbonate-utils",
    version="0.4.0",
    author="Brice Arnould",
    author_email="b.arnould@criteo.com",
    maintainer="Corentin Chary",
    maintainer_email="c.chary@criteo.com",
    description=("Tools for tools for managing federated carbon clusters."),
    license="MIT",
    keywords="graphite carbon carbonate",
    url="https://github.com/criteo/carbonate-utils",
    include_package_data=True,
    py_modules=['carbonate_sync'],
    long_description=read('README.md'),
    install_requires=[
      "carbon",
      "whisper",
      "carbonate",
      "progressbar2",
    ],
    test_suite='tests',
    cmdclass={'install_scripts': my_install_scripts},
    entry_points={
        'console_scripts': [
            'carbonate-sync = carbonate_sync:main',
            ]
        }
    )
