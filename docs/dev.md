# Python Toolkit for WMO BUFR Messages


## Design
The system is composed of two major components, the Script and the VM. In
general, the VM provides processing functions and the Script uses the function
with instructions. In another word, the Script weaves the VM functions and
controls the program workflow. It is also worth to note that the VM and Script
concept is loosely defined and they are not supposed to be similar to VMs for
programming languages. The concept is used to help understand how the software
works.

### The VM
The VM is not a traditional sense of VM that provides low level instructions
for executing byte codes. Rather it provides high level functions that each
perform a unit of data decoding or encoding action. For an example,
`decode_string_uncompressed` is a function to specifically decode a string
value from an uncompressed BUFR data binary stream.

### The Script
The script is composed of a series of statements that drive the VM. It is
written a custom language called the BUFR Processing Command Language (BPCL)
that is detailed in later sections.


## BUFR Processing Command Language
The BUFR Processing Command Language (BPCL) is a small language that helps make
the Toolkit more flexible. The language compiles down to Python AST which makes
it possible to use many Python features, especially builtin functions.

The runtime logic is entirely driven by BPCL scripts. The toolkit comes with
several BPCL scripts that facilitates regular tasks, e.g. decoding and encoding.
Users can also specify their own BPCL scripts to gain finer control of the
process.

An example of program control flow is as the follows:
1. VM prepares the Global Namespace to expose itself to BPCL scripts
2. VM loads BPCL script based on its type and initialisation instruction
3. VM executes loaded BPCL script (from here, the BPCL script takes control)
    * Exactly how a BUFR message should be read and decoded is controlled
      by the commands and functions in the BPCL script.
    * A BPCL script controls the logic of the processing. For the actual
      processing it still calls to the VM to do the heavy lifting.
    * A usual command from the BPCL script is to compile the BUFR Template
      to create the function used for the actual processing.
6. VM regains control once BPCL script completes. It then performs finalising 
   and housekeeping tasks depend on the VM type and initialisation instructions.
   
   
## BUFR Template Compilation
A BUFR Template is composed of one or more BUFR descriptors. It dictates a
streaming style processing for BUFR data. The compilation transforms a given
Template to a series of actions and package them in the form of Python
functions/methods.

The compiled template can be saved as Python source code. This source code can
be loaded at a later time when the same template is to be processed again to
boost performance.