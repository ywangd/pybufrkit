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
provided directory. The naming convention of the files is as the follows::

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

Descriptors and Their IDs
~~~~~~~~~~~~~~~~~~~~~~~~~
In addition to the four canonical types of Descriptor defined by the BUFR Spec,
this toolkit defines a few more Descriptors to help organise the decoded data.
These new Descriptors are listed as follows:

* **Associated Descriptor** - This descriptor is created for the associated field
  that a Element Descriptor may have. Its 6-character ID is almost the same as
  the Element Descriptor except starting with a letter **A**. For an example,
  the ID of Associated Descriptor for Element Descriptor ``015037`` is ``A15037``.

* **Skipped Local Descriptor** - This descriptor is used in place of any skipped
  local descriptors. This descriptor's ID begins with a letter **S** and the other
  5 digits are the same of the descriptor that is skipped.

* **Marker Descriptors** - This is a group of Descriptors that are used in place
  of marker values such as substitution, first order stats etc. Their ID begins
  with **T**, **F**, **D**, and **R** for Substitution, First Order Stats,
  Difference Stats and Replacement/Retain, respectively. The other 5 digits are
  the same as the Element Descriptor they are associated via Bitmap.

Query BUFR Messages
^^^^^^^^^^^^^^^^^^^
Queries can be performed against either the metadata sections (section 0, 1, 2,
3) or the data section (the Template data). Though they are implemented
separately in the backend, they share the same command line interface. This is
made possible by requiring metadata query expression to always start with a
percentage sign (%).

Query the Metadata Section
~~~~~~~~~~~~~~~~~~~~~~~~~~
The following is the
`EBNF <https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form>`_
form of query expressions for metadata sections::

    <query_expr> = '%'[<section_index>.]<parameter_name>

where the ``parameter_name`` are those defined in the configuration files,
e.g. ``n_subsets``, ``edition``, etc.

The metadata query always return a scalar value. For parameters that are common
across multiple sections, e.g. ``section_length``, the first entry will be
returned by default. For an example, the parameter ``section_length`` appears in
Secton 1, 2, 3, and 4. By default, the entry of Section 1 is queried and its
value is returned. To explicitly specify a Section, a ``section_index`` can be
added in between the percentage sign and the ``parameter_name``, e.g.
``%2.section_length`` returns the parameter value from Section 2 instead of 1.

Query the Template Data
~~~~~~~~~~~~~~~~~~~~~~~
The following is the
`EBNF <https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form>`_
form of query expressions for template data::

    <query_expr> = [<subset_spec>] <path_spec>+
    <subset_spec> = '@'<slice>
    <path_spec> = <separator> <descriptor_id> [<slice>]
    <separator> = '/' | '.' | '>'

* The ``<slice>`` takes the same syntax as how Python list can be sliced,
  e.g. ``[1]``, ``[-1]``, ``[:]``, ``[::10]``.

* The ``<descriptor_id>`` is the 6-letter/digit (leading zeros are required) descriptor ID,
  e.g. ``001001``, ``301001``, ``A21062``.

* The ``<separator>`` can be omitted and defaults to ``>`` if a query string begins
  with a ``<path_spec>``.

* Whitespaces are ignored.

The followings list a few examples of valid query expressions:

* ``008042`` - All instances of descriptor ``008042`` regardless of where it appears.
  This form is equivalent to ``> 008042``.

* ``@[0] > 008042`` - Similar to the above query but only against the first subset.

* ``/008042`` - Only those that are root element of a BUFR Template

* ``/008042[0]`` - Similar to the above query but retrieve only the first instance.
  Note that the index does not account for the repetition of a descriptor in replication
  blocks, i.e. the descriptor will only be counted once.

* ``303051/008042`` - Only those that are direct children of ``303051``

* ``103000.031001`` - The delayed replication factor value of replication ``103000``.
  Note the separator between a delayed replication and its factor is a Dot.

* ``021062.A21062`` - The associated field of descriptor ``021062``.

The query is performed against the wired hierarchical Template Data, which is
*expanded*, *enhanced* and *populated*. These are explained as the follows:

* *Expanded* - The unexpanded descriptors are fully expanded. For an example, the
  sequence descriptor ``301001`` is expanded to contain two child descriptors,
  ``001001`` and ``001002``. The hierarchical structure is also kept so that
  the child descriptor can be accurately specified using the Slash (``/``) separator.

* *Enhanced* - Associated fields, first order stats, bitmapped descriptors are
  wired as attributes to their owner descriptors. The attributes relationship
  can be queried using the Dot (``.``) separator.

* *Populated* - The Template is populated with actual data from the Data section.
  If a descriptor is not populated, for an example, a delayed replication block
  may have Zero replication, an empty list will be returned when any of its
  children is queried.

Script Support
^^^^^^^^^^^^^^
Built upon the query feature, the script feature enables more flexible usage of
the toolkit. The feature leverages full power of Python by embedding query
expressions and injecting additional variables into normal Python code. For
example, the following script filters for files that uses BUFR Template 309052::

    if 309052 in ${%unexpanded_descriptors}: print(PBK_FILENAME)

Note that the query expressions are embedded into the code by enclosing them
inside ``${...}``. Also ``PBK_FILENAME`` is an extra variable injected by the
toolkit to hold the name of current file being processed.
Note you must use the function version of ``print``. This is due to the use of
``__future__`` import in the code. But otherwise no Python 3 syntax is enforced.

You can also embed data queries like the follows::

    print(${005001}, ${006001})

The above script prints latitude and longitude values from given BUFR files.
One thing to note about data values is that they are by nature hierarchical.
A file could contain multiple subsets, each subset could have replications.
So the raw form of data values are nested list. However nested lists are
rather difficult to work with and sometimes unnecessary. So it is possible
to specify the nesting level of data values so they are easier to work with.
By default, all values are turned into a simple list without any nesting.
For an example, if each subset has one value for the given query, a list
of N scalar values will be return with N equals to the number of subsets.
This is referred as nesting level One as there is only one level of parenthesis
for the returned value. All available nesting levels are:

* 0 - No parenthesis, only the first value will be returned as a scalar
  (all other values, if any, are simply dropped)
* 1 - One level of parenthesis (default). Values from all subsets are simply
  flattened into one simple list.
* 2 - Two level of parenthesis. Values from each subset are flatten into
  its own list, which is itself an element of the final return value.
* 4 - Fully hierarchical. No flatten at all. Each subset or replication have
  its own parenthesis grouping.

The above settings can be controlled via the command line option, ``-n`` or
``--data-values-nest-level``. Alternatively it can also be specified with
the script itself using following magic comment at the beginning::

    #$ data_values_nest_level = 0

Note the magic comment line starts with ``#$`` and must appears before any
other lines. The option passed from command line takes precedence over
the option from the script itself.

Renderer
^^^^^^^^
This component is responsible for rendering the processed BUFR message object
in different formats, e.g. plain text, JSON.