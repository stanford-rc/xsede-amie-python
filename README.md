pyamie
======

**pyamie** is a simple Python library that creates an AMIE database abstraction
layer to ease the implementation of AMIE packets by local XSEDE sites.

Local sites (also known as Service Providers) still have to implement handling
of AMIE packet by editing some Python scripts (see below).

About AMIE
==========

The Account Management Information Exchange (AMIE) software system provides
the capability for XSEDE to manage accounts and track resource usage.
AMIE is based on the [Global Grid Forum (GGF) Usage Records
specification](https://www.ogf.org/documents/GFD.98.pdf) and is used by
[XSEDE](https://www.xsede.org/) to provide a distributed accounting mechanism
across all of its sites so that an individual users usage can be tracked
across multiple resources. AMIE is also responsible for propagating
Distinguished Names across the XSEDE sites.

AMIE source code and reference implementation can be found
[here](http://software.xsede.org/production/amie/).


Example scripts
===============

**pyamie** still requires that local sites implement their own way of handling
AMIE packets (like creating an user account after receiving a packet of type
`request_account_create`), but without having to deal with the AMIE database.

The real work by local sites is to properly implement those scripts.

Two example scripts are provided in the `examples` directory:
* [`amie_example_proc.py`](examples/processing/amie_example_proc.py) is an
  example script derived from the AMIE implementation developed for Stanford's
  XStream GPU cluster. This script features a Python class where you implement
  your packet handler methods. It also shows how to implement Slack
  notifications when packets are handled or in case of failure.
* [`report_slurm_gpu_usage.py`](examples/usage/report_slurm_gpu_usage.py)
  is a script that does also use the **pyamie** library to demontrate how SUs
  are reported in the case of a GPU cluster like XStream (which does actually
  report GPU hours instead of CPU hours).

Additional `*.sh` scripts are provided as examples on how to launch the
above Python scripts.

In any case, testing your implementation using a local test TGCDB is highly
recommended.

To install **pyamie**:

    $ pip install --user git+git://github.com/stanford-rc/xsede-amie-python.git

Acknowledgements
================

Special thanks to Michael Shapiro who helped us to setup a local test TGCDB and
provided some guidance regarding AMIE.

Author
======

Stephane Thiell - [Stanford Research Computing Center](https://srcc.stanford.edu/)

License
=======

**pyamie** is released under the [LGPL 3.0](LICENSE).

This software is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.

Dependent Modules
=================

This code has the following dependencies
above and beyond the Python standard library:

Library:

* [psycopg2](http://initd.org/psycopg/) - LGPL 3.0

Example scripts:

* [ClusterShell](http://clustershell.readthedocs.io/en/latest/) - CeCILL-C License (LGPL compatible)
* [Requests](http://docs.python-requests.org/en/master/) - Apache2 License
* [PyYAML](http://pyyaml.org/) - MIT License
