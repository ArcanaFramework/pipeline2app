# Pipeline2App

[![Tests](https://github.com/ArcanaFramework/pipeline2app/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/ArcanaFramework/pipeline2app/actions/workflows/ci-cd.yml)
[![Codecov](https://codecov.io/gh/ArcanaFramework/pipeline2app/branch/main/graph/badge.svg?token=UIS0OGPST7)](https://codecov.io/gh/ArcanaFramework/pipeline2app)
[![Python versions](https://img.shields.io/pypi/pyversions/pipeline2app.svg)](https://pypi.python.org/pypi/pipeline2app/)
[![Latest Version](https://img.shields.io/pypi/v/pipeline2app.svg)](https://pypi.python.org/pypi/pipeline2app/)
[![Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat)](https://arcanaframework.github.io/pipeline2app)

<img src="./docs/source/_static/images/logo_small.png" alt="Logo Small" style="float: right;">

[Pipeline2App](http://arcanaframework.github.io/pipeline2app) is a tool for quickly turning
[Pydra](http://pydra.readthedocs.io) tasks and workflows into containerised applications
(e.g. [BIDS](http://bids.neuroimaging.io/) Apps or [XNAT](http://xnat.org) pipelines)

## Documentation

Detailed documentation on Pipeline2App can be found at [https://arcanaframework.github.io/pipeline2app](https://arcanaframework.github.io/pipeline2app)

## Installation

Pipeline2App can be installed for Python 3 using *pip*:

```bash
python3 -m pip install pipeline2app
```

This will enable you run basic apps against generic directory trees, however, if you want
to build specific apps you will also need to install the appropriate extension package, e.g.

```bash
python3 -m pip install pipeline2app-xnat
```

## License

This work is licensed under a
[Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-nc-sa/4.0/)

[![Creative Commons License: Attribution-NonCommercial-ShareAlike 4.0 International](https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-nc-sa/4.0/)

## Acknowledgements

The authors acknowledge the facilities and scientific and technical assistance of the National Imaging Facility, a National Collaborative Research Infrastructure Strategy (NCRIS) capability.
