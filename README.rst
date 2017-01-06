Python Toolkit for WMO BUFR Messages
====================================

`PyBufrKit <https://github.com/ywangd/pybufrkit>`_ is a **pure** Python package
to work with WMO BUFR (FM-94) messages. It can be used as both a
command line tool or library to decode and encode BUFR messages. Here is a brief
list of some of the features:

* Handles both compressed and un-compressed messages
* Handles all practical operator descriptors, including data quality info,
  stats, bitmaps, etc.
* Tested with the same set of BUFR files used by
  `ecCodes <https://software.ecmwf.int/wiki/display/ECC/ecCodes+Home>`_
  and `BUFRDC <https://software.ecmwf.int/wiki/display/BUFR/BUFRDC+Home>`_.

Find more documentation at http://pybufrkit.readthedocs.io/

Installation
------------
PyBufrKit is compatible with both Python 2.6+ and 3.5+. To install from PyPi::

    pip install pybufrkit

Or from source::

    python setup.py install

Command Line Usage
------------------

The command line usage of the toolkit takes the following form::

    pybufrkit [OPTIONS] sub-command ...

where the ``sub-command`` is one of following actions that can be performed by the tool:

* ``decode`` - Decode a BUFR file to outputs of various format, e.g. JSON
* ``encode`` - Encode a BUFR file from a JSON input
* ``info`` - Decode a BUFR file upt to the data associated to the BUFR Template (exclusive)
* ``lookup`` - Look up information about the given list of comma separated BUFR descriptors
* ``compile`` - Compile the given comma separated BUFR descriptors

Here are a few examples using the tool from command line. For more details, please refer
to the help option, e.g. ``pybufrkit decode -h``. Also checkout the
`documentation <http://pybufrkit.readthedocs.io/>`_.

* Decode a BUFR file
    ``pybufrkit decode BUFR_FILE``

* Decode a BUFR file and display it as a hierarchical structure corresponding to
  the BUFR Descriptors. In addition, the attribute descriptors are associated to
  their (bitmap) corresponding descriptors.

    ``pybufrkit decode -a BUFR_FILE``

* Decode a BUFR file and convert to JSON format (the JSON can be encoded back to the BUFR format)
    ``pybufrkit decode -j BUFR_FILE``
    
* Encode a JSON file to BUFR
    ``pybufrkit encode JSON_FILE BUFR_FILE``

* Decoded a BUFR file to JSON, pipe it to the encoder to encode it back to BUFR
    ``pybufrkit decode -j BUFR_FILE | pybufrkit encode -``

* Decode only the metadata sections of a BUFR file
    ``pybufrkit info BUFR_FILE``

* Lookup information for a Element Descriptor (along with its code table)
    ``pybufrkit lookup -l 020003``

* Compile a BUFR Template composed as a comma separated list of descriptors
    ``pybufrkit compile 309052,205060``

Library Usage
-------------

The followings are some basic library usage::

    # Decode a BUFR file
    from pybufrkit.decoder import Decoder
    decoder = Decoder()
    with open(SOME_BUFR_FILE, 'rb') as ins:
        bufr_message = decoder.process(ins.read())

    # Convert the BUFR message to JSON
    from pybufrkit.renderer import FlatJsonRenderer
    json_string = FlatJsonRenderer().render(bufr_message)

    # Encode the JSON back to BUFR file
    from pybufrkit.encoder import Encoder
    encoder = Encoder()
    bufr_message_new = encoder.process(json_string)
    with open(BUFR_OUTPUT_FILE, 'wb') as outs:
        outs.write(bufr_message_new.serialized_bytes)


