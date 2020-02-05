"""
pybufrkit.coder
~~~~~~~~~~~~~~~

"""
from __future__ import absolute_import
from __future__ import print_function

import logging
import abc
import functools
from collections import namedtuple

# noinspection PyUnresolvedReferences
from six.moves import range, zip

from pybufrkit.constants import (DEFAULT_TABLES_DIR,
                                 UNITS_CODE_TABLE,
                                 UNITS_FLAG_TABLE,
                                 UNITS_STRING)
from pybufrkit.errors import PyBufrKitError, UnknownDescriptor
from pybufrkit.bufr import SectionConfigurer
from pybufrkit.descriptors import (ElementDescriptor,
                                   FixedReplicationDescriptor,
                                   DelayedReplicationDescriptor,
                                   OperatorDescriptor,
                                   SequenceDescriptor,
                                   AssociatedDescriptor,
                                   MarkerDescriptor,
                                   SkippedLocalDescriptor)

# Bitmap definition stage
BITMAP_NA = 0  # e.g. not in a bitmap definition block
BITMAP_INDICATOR = 1  # e.g. 222000, 223000
BITMAP_WAITING_FOR_BIT = 4
BITMAP_BIT_COUNTING = 5  # processing 031031

# STATUS of QA info follows, 222000
QA_INFO_NA = 0  # no in QA info follows range
QA_INFO_WAITING = 1  # after seeing 222000
QA_INFO_PROCESSING = 2  # after seeing the first class 33 descriptor

# Modifier for nbits, scale and reference value
BSRModifier = namedtuple('BSRModifier',
                         ['nbits_increment', 'scale_increment', 'refval_factor'])

log = logging.getLogger(__file__)


class AuditedList(list):
    """
    This class provides wrappers for some list methods, e.g. append, so that
    it is possible to execute additional code when the method is invoked.
    It is used mainly for debug purpose.
    """

    def append(self, p_object):
        log.debug('{!r}'.format(p_object))
        super(AuditedList, self).append(p_object)

    def __getitem__(self, item):
        value = super(AuditedList, self).__getitem__(item)
        log.debug('{!r}'.format(value))
        return value


class CoderState(object):
    """
    The state of Coder for keeping track of variables when a Coder is working. The use
    of a new state for each run makes it possible to use a single Coder to run
    multiple decoding/encoding tasks.

    :param decoded_values_all_subsets: This is only for Encoder use.
    """

    def __init__(self, is_compressed, n_subsets, decoded_values_all_subsets=None):

        self.is_compressed = is_compressed
        self.n_subsets = n_subsets

        self.idx_subset = 0

        # bitmap_links is a dictionary with key being the index to an attribute descriptor,
        # e.g. QA info, Stats, and the value being the index to the descriptor that the
        # attribute should be attached to.

        if is_compressed:
            # Compressed data has exactly the same decoded descriptors for each subset
            # Hence elements in the all_subsets list reference to the same object.
            self.decoded_descriptors_all_subsets = [[]] * n_subsets
            self.bitmap_links_all_subsets = [{}] * n_subsets
        else:
            # For Uncompressed data, each element of all_subsets are independent
            self.decoded_descriptors_all_subsets = [[] for _ in range(n_subsets)]
            self.bitmap_links_all_subsets = [{} for _ in range(n_subsets)]

        # For uncompressed data, the following two values will be changed during
        # subset context switching
        self.decoded_descriptors = [] if n_subsets == 0 else self.decoded_descriptors_all_subsets[0]
        self.bitmap_links = [] if n_subsets == 0 else self.bitmap_links_all_subsets[0]

        # When debug is turned on, use AuditedList for more logging messages.
        # Each element in the values all_subsets is different compressed or not
        if logging.root.level == logging.getLevelName('DEBUG'):
            if decoded_values_all_subsets:
                self.decoded_values_all_subsets = [AuditedList(vals) for vals in decoded_values_all_subsets]
            else:
                self.decoded_values_all_subsets = [AuditedList() for _ in range(n_subsets)]
        else:
            self.decoded_values_all_subsets = decoded_values_all_subsets or [[] for _ in range(n_subsets)]

        self.decoded_values = [] if n_subsets == 0 else self.decoded_values_all_subsets[0]

        self.idx_value = 0  # only needed for encoder

        self.nbits_offset = 0  # 201
        self.scale_offset = 0  # 202

        self.nbits_of_new_refval = 0  # 203
        self.new_refvals = {}  # 2 03 255 to conclude, not cancel

        self.nbits_of_associated = []  # 204
        self.nbits_of_skipped_local_descriptor = 0  # 206

        self.bsr_modifier = BSRModifier(
            nbits_increment=0, scale_increment=0, refval_factor=1
        )  # 207

        self.new_nbytes = 0  # 208

        self.data_not_present_count = 0  # 221
        self.status_qa_info_follows = QA_INFO_NA  # 222

        # bitmap definition
        self.bitmap = None
        self.bitmapped_descriptors = None
        self.bitmap_definition_state = BITMAP_NA
        self.most_recent_bitmap_is_for_reuse = False
        self.n_031031 = 0

        # Function to retrieve next bitmapped descriptor. Will be defined when
        # a bitmap is created or recalled.
        self.next_bitmapped_descriptor = None

        # Where to start count back for bitmap related descriptors
        self.back_reference_boundary = 0
        self.back_referenced_descriptors = None

    # noinspection PyAttributeOutsideInit
    def switch_subset_context(self, idx_subset):
        """
        This function is only useful for uncompressed data.
        """
        self.idx_subset = idx_subset
        # Reset new reference values to empty at start of each subset as anything defined
        # from previous subset should NOT affect this subset. Also we do not
        # care about what is defined in previous subset so we are not saving them.
        self.new_refvals = {}
        self.decoded_descriptors = self.decoded_descriptors_all_subsets[idx_subset]
        self.decoded_values = self.decoded_values_all_subsets[idx_subset]
        self.bitmap_links = self.bitmap_links_all_subsets[idx_subset]
        # Index to value is only needed for encoder
        self.idx_value = 0

    def mark_back_reference_boundary(self):
        self.back_reference_boundary = len(self.decoded_descriptors)

    def recall_bitmap(self):
        self.next_bitmapped_descriptor = functools.partial(next, iter(self.bitmapped_descriptors))
        return self.bitmap

    def cancel_bitmap(self):
        self.bitmap = None

    def cancel_all_back_references(self):
        self.back_referenced_descriptors = None
        self.bitmap = None
        self.bitmapped_descriptors = None

    def add_bitmap_link(self):
        """
        Must be called before the descriptor is processed
        """
        idx_descriptor, _ = self.next_bitmapped_descriptor()
        self.bitmap_links[len(self.decoded_descriptors)] = idx_descriptor

    def get_value_for_delayed_replication_factor(self, idx):
        if self.is_compressed:
            self._assert_equal_values_of_index(idx)
            value = self.decoded_values_all_subsets[0][idx]
        else:
            value = self.decoded_values[idx]

        if value is None or value < 0:
            raise PyBufrKitError('Delayed replication factor must be >= 0: got ({!r})'.format(value))

        return value

    def build_bitmapped_descriptors(self, bitmap):
        """
        Build the bitmapped descriptors based on the given bitmap. Also build
        the back referenced descriptors if it is not already defined.
        """
        # Second get all the back referenced descriptors if it does not already exist
        if not self.back_referenced_descriptors:
            self.back_referenced_descriptors = []
            for idx in range(self.back_reference_boundary - 1, -1, -1):
                descriptor = self.decoded_descriptors[idx]
                # The type has to be an exact match, not just isinstance
                if type(descriptor) is ElementDescriptor:
                    self.back_referenced_descriptors.insert(0, (idx, descriptor))
                    if len(self.back_referenced_descriptors) == len(bitmap):
                        break
        if len(self.back_referenced_descriptors) != len(bitmap):
            raise PyBufrKitError('Back referenced descriptors not matching defined Bitmap')

        # Lastly, get all the descriptors that has a corresponding Zero bit value
        self.bitmapped_descriptors = [
            (idx, d) for bit, (idx, d) in zip(
                bitmap,
                self.back_referenced_descriptors
            ) if bit == 0
        ]
        self.next_bitmapped_descriptor = functools.partial(next, iter(self.bitmapped_descriptors))

    def _assert_equal_values_of_index(self, idx):
        """
        Assert that the values of the specified index are identical for all
        subsets. It is only used for compressed data. For an example, to ensure
        the delayed replication factors are the same for all subsets.
        """
        minv, maxv = CoderState.minmax([values[idx] for values in self.decoded_values_all_subsets])
        assert minv == maxv, 'Values from all subsets are NOT identical'

    @staticmethod
    def minmax(values):
        """
        Give a list of values, find out the minimum and maximum, ignore any Nones.
        """
        mn, mx = None, None
        for v in values:
            if v is not None:
                if mn is None or mn > v:
                    mn = v
                if mx is None or mx < v:
                    mx = v
        return mn, mx


class Coder(object):
    """
    This class is an abstract superclass for Decoder and Encoder. By itself it
    cannot do anything. But it provides common operations for subclasses.

    :param definitions_dir: Where to find the BPCL definition files.
    :param tables_root_dir: Where to find the BUFR table files.
    """

    def __init__(self,
                 definitions_dir=None,
                 tables_root_dir=None):

        self.section_configurer = SectionConfigurer(definitions_dir=definitions_dir)
        self.tables_root_dir = tables_root_dir or DEFAULT_TABLES_DIR

    @abc.abstractmethod
    def process(self, *args, **kwargs):
        """Entry point of the class"""

    @abc.abstractmethod
    def process_section(self, bufr_message, bit_operator, section):
        """
        Process the given section of a BUFR message

        :param bufr_message: The BufrMessage object to process
        :param bit_operator: The bit operator (reader or writer)
        :param section:
        """

    def process_template(self, state, bit_operator, template):
        """
        Process the top level BUFR Template

        :param state: The state of the processing.
        :param bit_operator: The bit operator for read/write bits.
        :param template: The BUFR Template of the message.
        """
        self.process_members(state, bit_operator, template.members)

    def process_members(self, state, bit_operator, members):
        """
        Process a list of descriptors that are members of a composite descriptor.

        :param state: The state of the processing.
        :param bit_operator: The bit operator for read/write bits.
        :param members: A list of descriptors.
        """
        for member in members:
            member_type = type(member)

            log.debug('Processing {} {}'.format(member, member.name if hasattr(member, 'name') else ''))

            # TODO: NOT using if-elif for following checks because they may co-exist???
            #      It is highly unlikely if not impossible

            # 221 YYY data not present for following YYY descriptors except class 0-9 and 31
            if state.data_not_present_count:
                state.data_not_present_count -= 1
                log.debug('Data not present: {} to go'.format(state.data_not_present_count))

                if member_type is ElementDescriptor:
                    X = member.X
                    if not (1 <= X <= 9 or X == 31):  # skipping
                        continue
                        # TODO: maybe the descriptor should still be kept and set its value to None?
                        #       So it helps to keep the structure intact??

            # Currently defining new reference values
            # For ElementDescriptor only. This makes sense though not explicitly stated in the manual
            if state.nbits_of_new_refval and member_type is ElementDescriptor:
                self.process_define_new_refval(state, bit_operator, member)
                continue

            # 206 YYY signify data width for local descriptor
            if state.nbits_of_skipped_local_descriptor:
                self.process_skipped_local_descriptor(state, bit_operator, member)
                continue

            # Currently defining new bitmap
            if state.bitmap_definition_state != BITMAP_NA:
                self.process_bitmap_definition(state, bit_operator, member)

            # Now process normally
            if member_type is ElementDescriptor:
                self.process_element_descriptor(state, bit_operator, member)

            elif member_type is FixedReplicationDescriptor:
                self.process_fixed_replication_descriptor(state, bit_operator, member)

            elif member_type is DelayedReplicationDescriptor:
                self.process_delayed_replication_descriptor(state, bit_operator, member)

            elif member_type is OperatorDescriptor:
                self.process_operator_descriptor(state, bit_operator, member)

            elif member_type is SequenceDescriptor:
                self.process_sequence_descriptor(state, bit_operator, member)

            else:
                raise UnknownDescriptor('Cannot process descriptor {} of type: {}'.format(
                    member, member_type.__name__))

    def process_define_new_refval(self, state, bit_operator, descriptor):
        """
        Process defining a new reference value for the given descriptor.

        :param state:
        :param bit_operator:
        :param descriptor:
        """
        log.debug('Defining new reference value for {}'.format(descriptor))
        if descriptor.unit == UNITS_STRING:
            raise PyBufrKitError('Cannot define new reference value for descriptor of string value')
        self.process_new_refval(state, bit_operator, descriptor, state.nbits_of_new_refval)

    def process_skipped_local_descriptor(self, state, bit_operator, descriptor):
        """
        Skip number of bits defined for the local descriptor.

        :param state:
        :param bit_operator:
        :param descriptor:
        """
        log.debug('Skipping {} bits for local descriptor {}'.format(
            state.nbits_of_skipped_local_descriptor, descriptor))

        # TODO: possible associated fields?
        self.process_codeflag(
            state, bit_operator,
            SkippedLocalDescriptor(descriptor.id, state.nbits_of_skipped_local_descriptor),
            state.nbits_of_skipped_local_descriptor
        )
        state.nbits_of_skipped_local_descriptor = 0  # reset back to zero

    def process_bitmap_definition(self, state, bit_operator, descriptor):
        """
        Process bitmap definition. This is basically done as a state machine
        for processing all the bits associated to the bitmap.

        :type state: CoderState
        :param bit_operator:
        :param descriptor:
        """
        if state.bitmap_definition_state == BITMAP_INDICATOR:
            # TODO: 236000 and 237000 are handled here. bad?
            if descriptor.id == 236000:  # bitmap defined for reuse
                log.debug('Defining bitmap for reuse')
                state.most_recent_bitmap_is_for_reuse = True
                state.bitmap_definition_state = BITMAP_WAITING_FOR_BIT
                state.n_031031 = 0

            elif descriptor.id == 237000:  # re-call most recent definition
                log.debug('Recall most recently defined bitmap')
                state.bitmap_definition_state = BITMAP_NA

            else:  # direct bitmap definition (non-reuse)
                log.debug('Defining non-reuse bitmap')
                state.most_recent_bitmap_is_for_reuse = False
                state.bitmap_definition_state = BITMAP_WAITING_FOR_BIT
                state.n_031031 = 0

        elif state.bitmap_definition_state == BITMAP_WAITING_FOR_BIT:
            if descriptor.id == 31031:
                state.bitmap_definition_state = BITMAP_BIT_COUNTING
                state.n_031031 += 1

        elif state.bitmap_definition_state == BITMAP_BIT_COUNTING:
            if descriptor.id == 31031:
                state.n_031031 += 1
            else:
                # TODO: for compressed data, ensure all bitmap is equal
                log.debug('Bitmap defined with {} bits'.format(state.n_031031))
                self.define_bitmap(state, state.most_recent_bitmap_is_for_reuse)
                state.bitmap_definition_state = BITMAP_NA

    def process_element_descriptor(self, state, bit_operator, descriptor):
        """
        Process an ElementDescriptor.

        :type state: CoderState
        :type bit_operator:
        :type descriptor: ElementDescriptor
        """
        X = descriptor.X

        # Read associated field if exists
        # Page 79 of layer 3 Guide, operators do not apply to class 31 element descriptor
        if state.nbits_of_associated and X != 31:
            log.debug('Processing associated field of {} bits'.format(state.nbits_of_associated))
            self.process_associated_field(state, bit_operator, descriptor)

        # Handle class 33 codes for QA information follows 222000 operator
        if X == 33:
            if state.status_qa_info_follows == QA_INFO_WAITING:
                state.status_qa_info_follows = QA_INFO_PROCESSING
            # Add the link between the QA info and its corresponding descriptor
            if state.status_qa_info_follows == QA_INFO_PROCESSING:
                state.add_bitmap_link()
        else:
            if state.status_qa_info_follows == QA_INFO_PROCESSING:
                state.status_qa_info_follows = QA_INFO_NA

        # Now we can process the element normally
        if descriptor.unit == UNITS_STRING:
            nbytes = state.new_nbytes if state.new_nbytes else descriptor.nbits // 8
            self.process_string(state, bit_operator, descriptor, nbytes)

        elif descriptor.unit in (UNITS_FLAG_TABLE, UNITS_CODE_TABLE):
            self.process_codeflag(state, bit_operator, descriptor, descriptor.nbits)

        else:
            nbits = (descriptor.nbits +
                     state.nbits_offset +
                     state.bsr_modifier.nbits_increment)
            scale = (descriptor.scale +
                     state.scale_offset +
                     state.bsr_modifier.scale_increment)
            scale_powered = 1.0 * 10 ** scale

            if descriptor.id not in state.new_refvals:  # no new refval is defined for this descriptor
                refval = descriptor.refval * state.bsr_modifier.refval_factor
                self.process_numeric(state, bit_operator, descriptor, nbits, scale_powered, refval)

            else:  # a new refval is defined for the descriptor, it must be retrieved at runtime
                self.process_numeric_of_new_refval(state, bit_operator,
                                                   descriptor, nbits, scale_powered,
                                                   state.bsr_modifier.refval_factor)

    def process_fixed_replication_descriptor(self, state, bit_operator, descriptor):
        """
        Process a fixed replication descriptor including all members belong to this
        replication structure.

        :param state:
        :param bit_operator:
        :type descriptor: FixedReplicationDescriptor
        """
        for _ in range(descriptor.n_repeats):
            self.process_members(state, bit_operator, descriptor.members)

    def process_delayed_replication_descriptor(self, state, bit_operator, descriptor):
        """
        Process the delayed replication factor descriptor.

        :param state:
        :param bit_operator:
        :type descriptor: DelayedReplicationDescriptor
        """
        # TODO: delayed repetition descriptor 031011, 031012
        if descriptor.id in (31011, 31012):
            raise NotImplementedError('delayed repetition descriptor')

        log.debug('Processing {}'.format(descriptor.factor))
        self.process_element_descriptor(state, bit_operator, descriptor.factor)
        for _ in range(self.get_value_for_delayed_replication_factor(state)):
            self.process_members(state, bit_operator, descriptor.members)

    def process_operator_descriptor(self, state, bit_operator, descriptor):
        """
        Process Operator Descriptor.

        :param state:
        :param bit_operator:
        :type descriptor: OperatorDescriptor
        """
        operator_code, operand_value = descriptor.operator_code, descriptor.operand_value

        if operator_code == 201:  # nbits offset
            state.nbits_offset = (operand_value - 128) if operand_value else 0

        elif operator_code == 202:  # scale offset
            state.scale_offset = (operand_value - 128) if operand_value else 0

        elif operator_code == 203:  # new reference value
            if operand_value == 255:  # 255 is to conclude not cancel
                state.nbits_of_new_refval = 0
            else:
                state.nbits_of_new_refval = operand_value
                if operand_value == 0:
                    state.new_refvals = {}

        elif operator_code == 204:  # associated field
            if operand_value == 0:
                state.nbits_of_associated.pop()
            else:
                state.nbits_of_associated.append(operand_value)

        elif operator_code == 205:  # read string of YYY bytes
            # TODO: Need take care of associated field?
            # TODO: this is not affected by nbytes_new 208 YYY
            self.process_string(state, bit_operator, descriptor, operand_value)

        elif operator_code == 206:  # skip local descriptor of YYY bits
            state.nbits_of_skipped_local_descriptor = operand_value

        elif operator_code == 207:  # increase nbits, scale, refval
            if operand_value == 0:
                state.bsr_modifier = BSRModifier(
                    nbits_increment=0, scale_increment=0, refval_factor=1
                )
            else:
                state.bsr_modifier = BSRModifier(
                    nbits_increment=(10 * operand_value + 2) // 3,
                    scale_increment=operand_value,
                    refval_factor=10 ** operand_value,
                )

        elif operator_code == 208:  # change all string type descriptor length
            state.new_nbytes = operand_value

        # Data not present for following YYY descriptors except class 0-9 and 31
        elif operator_code == 221:
            state.data_not_present_count = operand_value

        # Quality info, substituted, 1st order stats, difference stats, replaced
        elif operator_code in (222, 223, 224, 225, 232):
            if operand_value == 0:
                state.bitmap_definition_state = BITMAP_INDICATOR
                state.mark_back_reference_boundary()
                self.process_constant(state, bit_operator, descriptor, 0)
                if operator_code == 222:
                    state.status_qa_info_follows = QA_INFO_WAITING
            else:  # 255 for markers (this does not apply to 222)
                self.process_marker_operator_descriptor(state, bit_operator, descriptor)

        elif operator_code == 235:
            state.cancel_all_back_references()

        elif operator_code == 236:
            self.process_constant(state, bit_operator, descriptor, 0)

        elif operator_code == 237:
            if operand_value == 0:
                state.recall_bitmap()

            else:  # 255 cancel re-used bitmap
                if state.most_recent_bitmap_is_for_reuse:
                    state.cancel_bitmap()
            self.process_constant(state, bit_operator, descriptor, 0)

        else:  # TODO: 241, 242, 243
            raise NotImplementedError('Operator Descriptor {} not implemented'.format(descriptor))

    def process_sequence_descriptor(self, state, bit_operator, descriptor):
        self.process_members(state, bit_operator, descriptor.members)

    def process_associated_field(self, state, bit_operator, descriptor):
        """

        :type state: CoderState
        :param bit_operator:
        :param descriptor:
        """
        nbits_associated = sum(state.nbits_of_associated)
        self.process_codeflag(state, bit_operator,
                              AssociatedDescriptor(descriptor.id, nbits_associated),
                              nbits_associated)

    def process_marker_operator_descriptor(self, state, bit_operator, descriptor):
        """

        :type state: CoderState
        :param bit_operator:
        :param descriptor:
        """
        # TODO: do we really need associated field for marker operators
        if state.nbits_of_associated:
            self.process_associated_field(state, bit_operator, descriptor)
        self.process_bitmapped_descriptor(state, bit_operator, descriptor)

    def process_bitmapped_descriptor(self, state, bit_operator, descriptor):
        """
        A generic method for processing bitmapped descriptors. It is wrapped by
        providing different funcs to handle encoding and decoding for
        uncompressed and compressed data.
        """

        idx_descriptor, bitmapped_descriptor = state.next_bitmapped_descriptor()
        state.bitmap_links[len(state.decoded_descriptors)] = idx_descriptor

        # difference statistical values marker has different refval and nbits values
        if descriptor.id == 225255:
            bitmapped_descriptor = MarkerDescriptor.from_element_descriptor(
                bitmapped_descriptor,
                descriptor.id,
                refval=-2 ** bitmapped_descriptor.nbits,
                nbits=bitmapped_descriptor.nbits + 1,
            )
        else:
            bitmapped_descriptor = MarkerDescriptor.from_element_descriptor(
                bitmapped_descriptor,
                descriptor.id,
            )

        self.process_element_descriptor(state, bit_operator, bitmapped_descriptor)

    @abc.abstractmethod
    def get_value_for_delayed_replication_factor(self, state):
        """
        Get value of the latest delayed replication factor. This is called when
        processing through the Template. But the actual implementation will be
        provided by sub-classes.

        :param state:
        :return: The value for the latest processed delayed replication factor
        """

    @abc.abstractmethod
    def define_bitmap(self, state, reuse):
        """
        Define a bit map.

        :param state:
        :param reuse: Is this bitmap for reuse?
        """

    @abc.abstractmethod
    def process_numeric(self, state, bit_operator, descriptor, nbits, scale_powered, refval):
        """
        Process a descriptor that has numeric value.

        :param descriptor: A BUFR descriptor that has numeric value
        :param nbits: Number of bits to process for the descriptor.
        :param scale_powered: 10 to the scale factor power, i.e. 10 ** scale
        :param refval: The reference value
        """

    @abc.abstractmethod
    def process_string(self, state, bit_operator, descriptor, nbytes):
        """
        Process a descriptor that has string value

        :param descriptor: The BUFR descriptor
        :param nbytes: Number of BYTES to process for the descriptor.
        """

    @abc.abstractmethod
    def process_codeflag(self, state, bit_operator, descriptor, nbits):
        """
        Process a descriptor that has code/flag value. A code/flag value
        does not need to scale and refval.

        :param descriptor: The BUFR descriptor
        :param nbits: Number of bits to process for the descriptor.
        """

    @abc.abstractmethod
    def process_new_refval(self, state, bit_operator, descriptor, nbits):
        """
        Process the new reference value for the given descriptor.

        :param descriptor: The BUFR descriptor.
        :param nbits: Number of bits to process.
        """

    @abc.abstractmethod
    def process_numeric_of_new_refval(self, state, bit_operator,
                                      descriptor, nbits, scale_powered,
                                      refval_factor):
        """
        Process a descriptor that has numeric value with new reference value.

        :param descriptor: The BUFR descriptor.
        :param nbits: Number of bits to process for the descriptor.
        :param scale_powered: 10 to the scale factor power, i.e. 10 ** scale
        :param refval_factor: The factor to be applied to the new refval.
        """

    @abc.abstractmethod
    def process_constant(self, state, bit_operator, descriptor, value):
        """
        Process a constant, with no bit operations, for the given descriptor.

        :param descriptor: The BUFR descriptor.
        :param value: The constant value.
        """
