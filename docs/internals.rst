How It Works
------------

BUFR Message Configuration files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The configurations describe how a BUFR message is composed of sections and how each
section is organised. They are used to provide the overall structure definition
of a BUFR message. The purpose of using configurations is to allow greater
flexibility so that changes of BUFR Spec do NOT (to a certain extent) require
program changes.

The builtin Configuration JSON files are located in the
`definitions <https://github.com/ywangd/pybufrkit/tree/master/pybufrkit/definitions>`_
directory inside the package. It can also be configured to load from an user
provided directory. The naming convention of the files is as follows::

    sectionX[-Y].json

where X is the section index, Y is the edition number and optional.

Each section is configured with some metadata and a list of parameters.
It takes the following general format::

    {
      "index": 0,  # zero-based section index

      "description": "Indicator section",

      "default": true,  # use this config if an edition-specific one is not available

      "optional": false, # whether this section is optional

      "end_of_message": false, # whether this is the last section

      "parameters": [  # a list of parameter configs
        {
          "name": "start_signature",  # parameter name

          "nbits": 32,  # number of bits

          "type": "bytes",  # parameter type determines how the value can be processed from the input bits

          "expected": "BUFR",  # expected value for this parameter (will be validated if not None)

          "as_property": false  # whether this parameter can be accessed from the parent message object
        },
        ...
      ]
    }

A few more notes about the configuration:

* Some section, e.g. Section 1, has edition-specific configuration, e.g. ``section1-1.json``.
  However, most sections have a single configuration for all editions. The ``default`` field
  is used to indicate that the configuration is a catch-all for any editions that does not
  have its own specific config.

* Number of bits can be set to ``0``, which means value of the corresponding parameter takes
  all the bits left for the section.

* There are generally two categories of parameter types, simple and complex.
    - Simple types are ``uint`` (unsigned integer), ``int`` (signed integer), ``bool``,
      ``bin`` (binary bits) and ``bytes`` (string).
    - Complex types include ``unexpanded_descriptors`` and ``template_data``. How they are
      processed is taken care of by a processor, e.g. Decoder. The configuration file does
      not concern how they are interpreted.

* The ``expected`` value will be validated against the actual value processed from the input.
  Currently, it is only used to ensure the start and stop signatures of a BUFR message.

* To allow loose coupling between sections, the parent message object can be configured to
  proxy some fields from a section. This is what the ``as_property`` field is
  for. For an example, the ``edition`` field from section 0 is needed for other
  sections to determine their structures. Therefore the ``edition`` field is
  proxyed by the parent message object so that it can be accessed by other
  sections without worrying about exactly which section provides this
  information.

Decoder and Encoder
^^^^^^^^^^^^^^^^^^^
These components process the input as prescribed by the configurations.
Each sections are processed in order of the section index. The components
also provide specialised methods to process parameters of complex types.
The processing of ``template_data`` is where most of the program logic goes.

The ``Decoder`` and ``Encoder`` are sub-classes of the same abstract ``Coder`` class.
They are implemented using the
`Template Method Pattern <https://en.wikipedia.org/wiki/Template_method_pattern>`_.
The ``Coder`` base class provides bootstrapping and common functions needed by all
of the sub-classes and leaves spaces for sub-classes to fill in the actual
processing of the parameters.

For an example, the base class knows how to process an Element Descriptor.
It prepares all necessary information about the descriptor, including its
type, number of bits, units, scale, reference, etc. Depending on its type,
the base class then invoke a method provided by the subclass to handle the
actual processing, which can be either decoding or encoding.

BUFR Template Compilation
^^^^^^^^^^^^^^^^^^^^^^^^^
The main purpose of Template Compilation is performance. However since bit
operations are the most time consuming part in the overall processing. The
performance gain somewhat is limited. Depending on the total number of
descriptors to be processed for a message, template compilation provides 10 -
30% performance boost. Larger number of descriptors, i.e. longer message,
generally lead to less performance gain.

The implementation of ``TemplateCompiler`` is similar to the Decoder/Encoder.
It is also a subclass of the abstract ``Coder`` class. It uses introspection
to record method calls dispatched by the base class. The recorded calls
can then be executed more efficiently because they bypass most of the
BUFR template processing, such as checking descriptor type, expand sequence
descriptors, etc.

Template Data Wiring
^^^^^^^^^^^^^^^^^^^^
A BUFR Template is by nature hierarchical. For an example, a sequence descriptor
has all of its child descriptors nested under it. When data associated to the
template is decoded, they can also be organised in a hierarchical format. This
is especially necessary when some operator descriptors, such as 204YYY
(associated field) and bitmap related operators (222, 223, 224, 225, 232, 235,
236, 237), make some values as attributes to other values. The wiring process
associates attributes to their owners so that their meanings are explicit.

Renderer
^^^^^^^^
This component is responsible for rendering the processed BUFR message object
in different formats, e.g. plain text, JSON.