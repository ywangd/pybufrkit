Python Toolkit for WMO BUFR Messages
====================================

.. image:: https://travis-ci.org/ywangd/pybufrkit.svg?branch=master
    :target: https://travis-ci.org/ywangd/pybufrkit

`PyBufrKit <https://github.com/ywangd/pybufrkit>`_ is a **pure** Python package
to work with WMO BUFR (FM-94) messages. It can be used as both a
command line tool or library to decode and encode BUFR messages. Here is a brief
list of some of the features:

* Pure Python
* Handles both compressed and un-compressed messages
* Handles all practical operator descriptors, including data quality info,
  stats, bitmaps, etc.
* Option to construct hierarchial structure of a message, e.g. associate
  first order stats data to their owners.
* Convenient subsetting support for BUFR messages
* Comprehensive query support for BUFR messages
* Script support enables flexible extensions, e.g. filtering through large number of files.
* Tested with the same set of BUFR files used by
  `ecCodes <https://software.ecmwf.int/wiki/display/ECC/ecCodes+Home>`_
  and `BUFRDC <https://software.ecmwf.int/wiki/display/BUFR/BUFRDC+Home>`_.

More documentation at http://pybufrkit.readthedocs.io/

An `online BUFR decoder <http://aws-bufr-webapp.s3-website-ap-southeast-2.amazonaws.com/>`_ powered by PyBufrKit, 
`Serverless <https://serverless.com/>`_ and `AWS Lambda <https://aws.amazon.com/lambda/>`_.

Installation
------------
PyBufrKit is compatible with Python 2.7, 3.5+, and `PyPy <https://pypy.org/>`_.
To install from PyPi::

    pip install pybufrkit

Or from source::

    python setup.py install

Command Line Usage
------------------

The command line usage of the toolkit takes the following form::

    pybufrkit [OPTIONS] command ...

where the ``command`` is one of following actions that can be performed by the tool:

* ``decode`` - Decode a BUFR file to outputs of various format, e.g. JSON
* ``encode`` - Encode a BUFR file from a JSON input
* ``info`` - Decode only the metadata sections (i.e. section 0, 1, 2, 3) of given BUFR files
* ``split`` - Split given BUFR files into one message per file
* ``subset`` - Subset the given BUFR file and save as new file
* ``query`` - Query metadata or data of given BUFR files
* ``script`` - Embed BUFR query expressions into normal Python script
* ``lookup`` - Look up information about the given list of comma separated BUFR descriptors
* ``compile`` - Compile the given comma separated BUFR descriptors

Here are a few examples using the tool from command line. For more details, please refer
to the help option, e.g. ``pybufrkit decode -h``. Also checkout the
`documentation <http://pybufrkit.readthedocs.io/>`_.

.. code-block:: Bash

    # Decode a BUFR file and output in the default flat text format
    pybufrkit decode BUFR_FILE

    # Decode a file that is a concatenation of multiple BUFR messages,
    # skipping any erroneous messages and continue on next one
    pybufrkit decode -m --continue-on-error FILE

    # Filter through a multi-message file and only decode messages
    # that have data_category equals to 2. See below for details
    # about usable filter expressions.
    pybufrkit decode -m --filter '${%data_category} == 2' FILE

    # Decode a BUFR file and display it in a hierarchical structure
    # corresponding to the BUFR Descriptors. In addition, the attribute
    # descriptors are associated to their (bitmap) corresponding descriptors.
    pybufrkit decode -a BUFR_FILE

    # Decode a BUFR file and output in the flat JSON format
    pybufrkit decode -j BUFR_FILE

    # Encode from a flat JSON file to BUFR
    pybufrkit encode -j JSON_FILE BUFR_FILE

    # Decode a BUFR file, pipe it to the encoder to encode it back to BUFR
    pybufrkit decode BUFR_FILE | pybufrkit encode -

    # Decode only the metadata sections of a BUFR file
    pybufrkit info BUFR_FILE

    # Split a BUFR file into one message per file
    pybufrkit split BUFR_FILE

    # Subset from a given BUFR file
    pybufrkit subset 0,3,6,9 BUFR_FILE

    # Query values from the metadata sections (section 0, 1, 2, 3):
    pybufrkit query %n_subsets BUFR_FILE

    # Query all values for descriptor 001002 of the data section
    pybufrkit query 001002 BUFR_FILE

    # Query for those root level 001002 of the BUFR Template
    pybufrkit query /001002 BUFR_FILE

    # Query for 001002 that is a direct child of 301001
    pybufrkit query /301001/001002 BUFR_FILE

    # Query for all 001002 of the first subset
    pybufrkit query '@[0] > 001002' BUFR_FILE

    # Query for associated field of 021062
    pybufrkit query 021062.A21062 BUFR_FILE

    # Filtering through a number of BUFR files with Script support
    # (find files that have multiple subsets):
    pybufrkit script 'if ${%n_subsets} > 1: print(PBK_FILENAME)' DIRECTORY/*.bufr

    # Lookup information for a Element Descriptor (along with its code table)
    pybufrkit lookup -l 020003

    # Compile a BUFR Template composed as a comma separated list of descriptors
    pybufrkit compile 309052,205060


Library Usage
-------------

The following code shows an example of basic library usage

.. code-block:: Python

    # Decode a BUFR file
    from pybufrkit.decoder import Decoder
    decoder = Decoder()
    with open(SOME_BUFR_FILE, 'rb') as ins:
        bufr_message = decoder.process(ins.read())

    # Convert the BUFR message to JSON
    from pybufrkit.renderer import FlatJsonRenderer
    json_data = FlatJsonRenderer().render(bufr_message)

    # Encode the JSON back to BUFR file
    from pybufrkit.encoder import Encoder
    encoder = Encoder()
    bufr_message_new = encoder.process(json_data)
    with open(BUFR_OUTPUT_FILE, 'wb') as outs:
        outs.write(bufr_message_new.serialized_bytes)

    # Decode for multiple messages from a single file
    from pybufrkit.decoder import generate_bufr_message
    with open(SOME_FILE, 'rb') as ins:
        for bufr_message in generate_bufr_message(decoder, ins.read()):
            pass  # do something with the decoded message object

    # Query the metadata
    from pybufrkit.mdquery import MetadataExprParser, MetadataQuerent
    n_subsets = MetadataQuerent(MetadataExprParser()).query(bufr_message, '%n_subsets')

    # Query the data
    from pybufrkit.dataquery import NodePathParser, DataQuerent
    query_result = DataQuerent(NodePathParser()).query(bufr_message, '001002')

    # Script
    from pybufrkit.script import ScriptRunner
    # NOTE: must use the function version of print (Python 3), NOT the statement version
    code = """print('Multiple' if ${%n_subsets} > 1 else 'Single')"""
    runner = ScriptRunner(code)
    runner.run(bufr_message)

**For more help, please check the documentation site at** http://pybufrkit.readthedocs.io/
